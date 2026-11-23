/* Fast non-blocking epoll MQTT 5 benchmark driver (plain TCP or TLS 1.3 mTLS via OpenSSL).
 * Supports up to 1024 pairs (2048 connections), QoS 0 / QoS 1, paced or max-throughput,
 * latency percentiles (p50/p95/p99), exact reconciliation (submitted/acked/refused/delivered).
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MAX_PAIRS 1024
#define MAX_CONNS (2 * MAX_PAIRS)
#define MAX_EVENTS 4000000
#define IO_BUF_SIZE 65536

static const uint64_t NS_PER_SEC = 1000000000ULL;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * NS_PER_SEC + (uint64_t)ts.tv_nsec;
}

static void die(const char *msg) {
    fprintf(stderr, "FATAL: %s (errno=%d: %s)\n", msg, errno, strerror(errno));
    exit(2);
}

static size_t encode_varint(uint8_t *out, size_t val) {
    size_t n = 0;
    do {
        uint8_t b = (uint8_t)(val % 128);
        val /= 128;
        if (val > 0) b |= 0x80;
        out[n++] = b;
    } while (val > 0);
    return n;
}

struct msg_rec {
    uint64_t scheduled_ns;
    uint64_t submitted_ns;
    uint64_t ack_ns;
    uint64_t recv_ns;
    int ack_reason;
    uint32_t copies;
};

struct conn {
    int fd;
    SSL *ssl;
    bool use_tls;
    bool is_pub;
    unsigned pair;
    char client_id[48];
    char topic[48];
    size_t topic_len;

    bool connected;
    bool subscribed;
    bool failed;

    uint16_t next_pid;
    unsigned inflight;
    unsigned max_inflight;
    uint32_t *pid_to_idx; /* 1-based msg index */

    uint8_t *rbuf;
    size_t rlen;
    size_t rcap;

    uint8_t *wbuf;
    size_t woff;
    size_t wlen;
    size_t wcap;
    bool want_write_ep;
};

struct bench {
    int epfd;
    SSL_CTX *pub_ctx;
    SSL_CTX *sub_ctx;
    bool use_tls;
    unsigned pairs;
    unsigned qos;
    unsigned payload_size;
    unsigned send_window;
    uint64_t count;
    struct msg_rec *recs;
    struct conn conns[MAX_CONNS];
    uint8_t *payload_template;

    uint64_t invalid;
    uint64_t disconnects;
    uint64_t stray_ack;
};

static void conn_append(struct conn *c, const uint8_t *data, size_t len) {
    if (c->wlen - c->woff + len > c->wcap) {
        if (c->woff > 0) {
            memmove(c->wbuf, c->wbuf + c->woff, c->wlen - c->woff);
            c->wlen -= c->woff;
            c->woff = 0;
        }
        if (c->wlen + len > c->wcap) {
            size_t ncap = c->wcap ? c->wcap * 2 : IO_BUF_SIZE;
            while (ncap < c->wlen + len) ncap *= 2;
            c->wbuf = realloc(c->wbuf, ncap);
            if (!c->wbuf) die("realloc wbuf");
            c->wcap = ncap;
        }
    } else if (c->wlen + len > c->wcap && c->woff > 0) {
        memmove(c->wbuf, c->wbuf + c->woff, c->wlen - c->woff);
        c->wlen -= c->woff;
        c->woff = 0;
    }
    memcpy(c->wbuf + c->wlen, data, len);
    c->wlen += len;
}

static void conn_update_ep(struct bench *b, struct conn *c) {
    if (c->fd < 0 || c->failed) return;
    bool need_w = (c->wlen > c->woff);
    if (need_w == c->want_write_ep) return;
    struct epoll_event ev = {0};
    ev.events = EPOLLIN | (need_w ? EPOLLOUT : 0);
    ev.data.ptr = c;
    epoll_ctl(b->epfd, EPOLL_CTL_MOD, c->fd, &ev);
    c->want_write_ep = need_w;
}

static void conn_close(struct bench *b, struct conn *c) {
    if (c->failed) return;
    c->failed = true;
    b->disconnects++;
    if (c->fd >= 0) {
        epoll_ctl(b->epfd, EPOLL_CTL_DEL, c->fd, NULL);
        if (c->ssl) {
            SSL_free(c->ssl);
            c->ssl = NULL;
        }
        close(c->fd);
        c->fd = -1;
    }
}

static void conn_flush(struct bench *b, struct conn *c) {
    while (!c->failed && c->wlen > c->woff) {
        size_t rem = c->wlen - c->woff;
        ssize_t n;
        if (c->use_tls) {
            n = SSL_write(c->ssl, c->wbuf + c->woff, (int)(rem > 65536 ? 65536 : rem));
            if (n <= 0) {
                int err = SSL_get_error(c->ssl, (int)n);
                if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) break;
                conn_close(b, c);
                return;
            }
        } else {
            n = send(c->fd, c->wbuf + c->woff, rem, MSG_NOSIGNAL);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) break;
                conn_close(b, c);
                return;
            }
            if (n == 0) {
                conn_close(b, c);
                return;
            }
        }
        c->woff += (size_t)n;
        if (c->woff == c->wlen) {
            c->woff = 0;
            c->wlen = 0;
        }
    }
    conn_update_ep(b, c);
}

static void send_connect(struct bench *b, struct conn *c) {
    uint8_t body[256];
    size_t p = 0;
    body[p++] = 0; body[p++] = 4;
    memcpy(body + p, "MQTT", 4); p += 4;
    body[p++] = 5; /* MQTT 5.0 */
    body[p++] = (b->qos == 0) ? 0x02 : 0x02; /* Clean Start = 1 */
    body[p++] = 0; body[p++] = 60; /* Keep alive = 60s */
    /* Properties: Receive Maximum = 64, Session Expiry = 0 */
    body[p++] = 3;
    body[p++] = 0x21;
    body[p++] = 0; body[p++] = 64;
    size_t cid_len = strlen(c->client_id);
    body[p++] = (uint8_t)(cid_len >> 8);
    body[p++] = (uint8_t)(cid_len & 0xff);
    memcpy(body + p, c->client_id, cid_len); p += cid_len;

    uint8_t pkt[270];
    size_t hp = 0;
    pkt[hp++] = 0x10;
    hp += encode_varint(pkt + hp, p);
    memcpy(pkt + hp, body, p);
    conn_append(c, pkt, hp + p);
    conn_flush(b, c);
}

static void send_subscribe(struct bench *b, struct conn *c) {
    uint8_t body[128];
    size_t p = 0;
    body[p++] = 0; body[p++] = 1; /* pid = 1 */
    body[p++] = 0; /* prop len */
    body[p++] = (uint8_t)(c->topic_len >> 8);
    body[p++] = (uint8_t)(c->topic_len & 0xff);
    memcpy(body + p, c->topic, c->topic_len); p += c->topic_len;
    body[p++] = (uint8_t)b->qos;

    uint8_t pkt[140];
    size_t hp = 0;
    pkt[hp++] = 0x82;
    hp += encode_varint(pkt + hp, p);
    memcpy(pkt + hp, body, p);
    conn_append(c, pkt, hp + p);
    conn_flush(b, c);
}

static bool send_publish(struct bench *b, struct conn *c, uint64_t idx) {
    if (c->failed) return false;
    if (b->qos == 1 && c->inflight >= c->max_inflight) return false;

    uint16_t pid = 0;
    if (b->qos == 1) {
        for (unsigned tries = 0; tries < 65535; tries++) {
            uint16_t cand = c->next_pid;
            c->next_pid = (cand == 65535) ? 1 : (uint16_t)(cand + 1);
            if (c->pid_to_idx[cand] == 0) {
                pid = cand;
                break;
            }
        }
        if (pid == 0) return false;
    }

    size_t body_len = 2 + c->topic_len + (b->qos ? 2 : 0) + 1 + b->payload_size;
    uint8_t hdr[128];
    size_t hp = 0;
    hdr[hp++] = (uint8_t)(0x30 | (b->qos << 1));
    hp += encode_varint(hdr + hp, body_len);
    hdr[hp++] = (uint8_t)(c->topic_len >> 8);
    hdr[hp++] = (uint8_t)(c->topic_len & 0xff);
    memcpy(hdr + hp, c->topic, c->topic_len); hp += c->topic_len;
    if (b->qos == 1) {
        hdr[hp++] = (uint8_t)(pid >> 8);
        hdr[hp++] = (uint8_t)(pid & 0xff);
    }
    hdr[hp++] = 0; /* property len = 0 */

    memcpy(b->payload_template, &idx, sizeof(idx));
    b->recs[idx].submitted_ns = now_ns();
    conn_append(c, hdr, hp);
    conn_append(c, b->payload_template, b->payload_size);
    if (b->qos == 1) {
        c->pid_to_idx[pid] = (uint32_t)(idx + 1);
        c->inflight++;
    }
    return true;
}

static void handle_packet(struct bench *b, struct conn *c, uint8_t first, const uint8_t *body, size_t len) {
    uint8_t ptype = first >> 4;
    if (ptype == 2) { /* CONNACK */
        if (len >= 2 && body[1] == 0) {
            /* Parse Receive Maximum if announced in CONNACK properties */
            if (len >= 3) {
                size_t pos = 2;
                size_t plen = 0, shift = 0;
                while (pos < len) {
                    uint8_t byte = body[pos++];
                    plen |= (size_t)(byte & 0x7f) << shift;
                    if ((byte & 0x80) == 0) break;
                    shift += 7;
                }
                size_t pend = pos + plen;
                while (pos < pend && pos < len) {
                    uint8_t pid_prop = body[pos++];
                    if (pid_prop == 0x21 && pos + 2 <= pend) {
                        uint16_t rm = ((uint16_t)body[pos] << 8) | body[pos + 1];
                        if (rm > 0 && rm < c->max_inflight) c->max_inflight = rm;
                        pos += 2;
                    } else if (pid_prop == 0x22 || pid_prop == 0x13) {
                        pos += 2;
                    } else if (pid_prop == 0x11 || pid_prop == 0x27) {
                        pos += 4;
                    } else if (pid_prop == 0x24 || pid_prop == 0x25 || pid_prop == 0x28 || pid_prop == 0x29 || pid_prop == 0x2a) {
                        pos += 1;
                    } else if (pid_prop == 0x12 || pid_prop == 0x1a || pid_prop == 0x1c || pid_prop == 0x1f) {
                        if (pos + 2 > pend) break;
                        uint16_t sl = ((uint16_t)body[pos] << 8) | body[pos + 1];
                        pos += 2 + sl;
                    } else {
                        break;
                    }
                }
            }
            c->connected = true;
            if (!c->is_pub) send_subscribe(b, c);
        } else {
            conn_close(b, c);
        }
    } else if (ptype == 9) { /* SUBACK */
        if (len >= 4) {
            c->subscribed = true;
        }
    } else if (ptype == 4) { /* PUBACK */
        if (len < 2 || !c->is_pub) return;
        uint16_t pid = ((uint16_t)body[0] << 8) | body[1];
        int reason = (len >= 3) ? body[2] : 0;
        uint32_t idx1 = c->pid_to_idx[pid];
        if (!idx1) {
            b->stray_ack++;
            return;
        }
        c->pid_to_idx[pid] = 0;
        c->inflight--;
        struct msg_rec *r = &b->recs[idx1 - 1];
        r->ack_ns = now_ns();
        r->ack_reason = reason;
    } else if (ptype == 3) { /* PUBLISH */
        uint8_t qos = (first >> 1) & 3;
        if (len < 2) { b->invalid++; return; }
        size_t tlen = ((size_t)body[0] << 8) | body[1];
        size_t pos = 2 + tlen;
        if (pos + (qos ? 2 : 0) + 1 > len) { b->invalid++; return; }
        uint16_t pid = 0;
        if (qos > 0) {
            pid = ((uint16_t)body[pos] << 8) | body[pos + 1];
            pos += 2;
        }
        /* skip MQTT 5 properties */
        size_t plen = 0, shift = 0;
        while (pos < len) {
            uint8_t byte = body[pos++];
            plen |= (size_t)(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) break;
            shift += 7;
        }
        pos += plen;
        if (pos + b->payload_size != len) { b->invalid++; return; }
        uint64_t idx = 0;
        memcpy(&idx, body + pos, sizeof(idx));
        if (idx >= b->count || idx % b->pairs != c->pair) {
            b->invalid++;
            return;
        }
        struct msg_rec *r = &b->recs[idx];
        if (r->copies++ == 0) {
            r->recv_ns = now_ns();
        }
        if (qos == 1) {
            uint8_t ack[4] = {0x40, 2, (uint8_t)(pid >> 8), (uint8_t)(pid & 0xff)};
            conn_append(c, ack, 4);
        }
    }
}

static void conn_read(struct bench *b, struct conn *c) {
    while (!c->failed) {
        if (c->rcap - c->rlen < 16384) {
            size_t ncap = c->rcap ? c->rcap * 2 : IO_BUF_SIZE;
            c->rbuf = realloc(c->rbuf, ncap);
            if (!c->rbuf) die("realloc rbuf");
            c->rcap = ncap;
        }
        ssize_t n;
        if (c->use_tls) {
            n = SSL_read(c->ssl, c->rbuf + c->rlen, (int)(c->rcap - c->rlen));
            if (n <= 0) {
                int err = SSL_get_error(c->ssl, (int)n);
                if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) break;
                conn_close(b, c);
                return;
            }
        } else {
            n = recv(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen, 0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) break;
                conn_close(b, c);
                return;
            }
            if (n == 0) {
                conn_close(b, c);
                return;
            }
        }
        c->rlen += (size_t)n;
        size_t off = 0;
        while (off + 2 <= c->rlen) {
            uint8_t first = c->rbuf[off];
            size_t pos = off + 1;
            size_t rem = 0, shift = 0;
            bool complete_hdr = false;
            for (int i = 0; i < 4 && pos < c->rlen; i++) {
                uint8_t byte = c->rbuf[pos++];
                rem |= (size_t)(byte & 0x7f) << shift;
                if ((byte & 0x80) == 0) {
                    complete_hdr = true;
                    break;
                }
                shift += 7;
            }
            if (!complete_hdr) break;
            if (pos + rem > c->rlen) break;
            handle_packet(b, c, first, c->rbuf + pos, rem);
            off = pos + rem;
        }
        if (off > 0) {
            if (off < c->rlen) memmove(c->rbuf, c->rbuf + off, c->rlen - off);
            c->rlen -= off;
        }
    }
    if (!c->failed && c->wlen > c->woff) conn_flush(b, c);
}

static SSL_CTX *make_tls_ctx(const char *pki, const char *role) {
    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) die("SSL_CTX_new");
    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
    char ca[512], cert[512], key[512];
    snprintf(ca, sizeof(ca), "%s/ca.crt", pki);
    snprintf(cert, sizeof(cert), "%s/%s.crt", pki, role);
    snprintf(key, sizeof(key), "%s/%s.key", pki, role);
    if (SSL_CTX_load_verify_locations(ctx, ca, NULL) != 1) die("load ca");
    if (SSL_CTX_use_certificate_file(ctx, cert, SSL_FILETYPE_PEM) != 1) die("load cert");
    if (SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM) != 1) die("load key");
    return ctx;
}

static void connect_blocking_then_nonblock(struct bench *b, struct conn *c, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) die("connect");

    c->fd = fd;
    c->use_tls = b->use_tls;
    c->next_pid = 1;
    c->max_inflight = b->send_window;
    if (c->is_pub && b->qos == 1) {
        c->pid_to_idx = calloc(65536, sizeof(uint32_t));
        if (!c->pid_to_idx) die("calloc pid");
    }
    if (b->use_tls) {
        c->ssl = SSL_new(c->is_pub ? b->pub_ctx : b->sub_ctx);
        SSL_set_fd(c->ssl, fd);
        if (SSL_connect(c->ssl) != 1) {
            ERR_print_errors_fp(stderr);
            die("SSL_connect");
        }
    }
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    struct epoll_event ev = {0};
    ev.events = EPOLLIN;
    ev.data.ptr = c;
    if (epoll_ctl(b->epfd, EPOLL_CTL_ADD, fd, &ev) != 0) die("epoll_ctl add");
}

static void poll_once(struct bench *b, int timeout_ms) {
    struct epoll_event evs[256];
    int n = epoll_wait(b->epfd, evs, 256, timeout_ms);
    for (int i = 0; i < n; i++) {
        struct conn *c = evs[i].data.ptr;
        if (c->failed) continue;
        if (evs[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR)) conn_read(b, c);
        if (!c->failed && (evs[i].events & EPOLLOUT)) conn_flush(b, c);
    }
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    if (argc < 8) {
        fprintf(stderr, "usage: arm64_bench PORT PKI_OR_NONE PAIRS QOS RATE DURATION_MS PAYLOAD_BYTES [WINDOW]\n");
        return 2;
    }
    int port = atoi(argv[1]);
    const char *pki = argv[2];
    bool use_tls = (strcmp(pki, "none") != 0);
    unsigned pairs = (unsigned)atoi(argv[3]);
    unsigned qos = (unsigned)atoi(argv[4]);
    unsigned rate = (unsigned)atoi(argv[5]);
    unsigned duration_ms = (unsigned)atoi(argv[6]);
    unsigned payload_size = (unsigned)atoi(argv[7]);
    unsigned send_window = (argc >= 9) ? (unsigned)atoi(argv[8]) : 64;

    if (pairs < 1 || pairs > MAX_PAIRS || payload_size < 8) die("invalid args");
    uint64_t count = (uint64_t)rate * duration_ms / 1000;
    if (count == 0 || count > MAX_EVENTS) die("invalid count");

    struct bench *b = calloc(1, sizeof(*b));
    if (!b) die("calloc bench");
    b->epfd = epoll_create1(0);
    b->use_tls = use_tls;
    b->pairs = pairs;
    b->qos = qos;
    b->payload_size = payload_size;
    b->send_window = send_window;
    b->count = count;
    b->recs = calloc(count, sizeof(*b->recs));
    b->payload_template = malloc(payload_size);
    if (!b->recs || !b->payload_template) die("alloc recs");
    memset(b->payload_template, 'A', payload_size);
    for (uint64_t i = 0; i < count; i++) b->recs[i].ack_reason = -1;

    if (use_tls) {
        b->pub_ctx = make_tls_ctx(pki, "sensor");
        b->sub_ctx = make_tls_ctx(pki, "dashboard");
    }

    /* Connect subscribers first, then publishers */
    for (unsigned i = 0; i < 2 * pairs; i++) {
        struct conn *c = &b->conns[i];
        c->fd = -1;
        c->pair = i % pairs;
        c->is_pub = (i >= pairs);
        snprintf(c->client_id, sizeof(c->client_id), "%s-%u", c->is_pub ? "sensor" : "dashboard", c->pair);
        snprintf(c->topic, sizeof(c->topic), "atelier/load/%u", c->pair);
        c->topic_len = strlen(c->topic);
        connect_blocking_then_nonblock(b, c, port);
        send_connect(b, c);
        if ((i & 15) == 15) poll_once(b, 1);
    }

    uint64_t ready_deadline = now_ns() + 15 * NS_PER_SEC;
    while (now_ns() < ready_deadline) {
        unsigned ready_cnt = 0;
        for (unsigned i = 0; i < 2 * pairs; i++) {
            struct conn *c = &b->conns[i];
            if (c->failed) die("client failed during handshake");
            if (c->connected && (c->is_pub || c->subscribed)) ready_cnt++;
        }
        if (ready_cnt == 2 * pairs) break;
        poll_once(b, 2);
    }
    for (unsigned i = 0; i < 2 * pairs; i++) {
        struct conn *c = &b->conns[i];
        if (!c->connected || (!c->is_pub && !c->subscribed)) die("handshake timeout");
    }

    uint64_t start = now_ns() + 20 * 1000000ULL;
    uint64_t end = start + (uint64_t)duration_ms * 1000000ULL;
    uint64_t drain_ms = (qos == 1) ? 15000ULL : 800ULL;
    uint64_t stop = end + drain_ms * 1000000ULL;
    for (uint64_t i = 0; i < count; i++) {
        b->recs[i].scheduled_ns = start + i * NS_PER_SEC / rate;
    }

    uint64_t next = 0;
    while (true) {
        uint64_t now = now_ns();
        if (now >= stop) break;
        unsigned sent_batch = 0;
        while (next < count && now < end && b->recs[next].scheduled_ns <= now && sent_batch < 128) {
            struct conn *pub = &b->conns[pairs + (next % pairs)];
            if (send_publish(b, pub, next)) {
                sent_batch++;
            } else {
                /* local window full: still advance schedule slot to match load_client semantics */
            }
            next++;
        }
        if (sent_batch > 0) {
            for (unsigned p = 0; p < pairs; p++) {
                struct conn *pub = &b->conns[pairs + p];
                if (!pub->failed && pub->wlen > pub->woff) conn_flush(b, pub);
            }
        }
        /* Flush subscriber PUBACKs and check whether post-window drain is complete. */
        if (now >= end) {
            bool any_pending = false;
            for (unsigned i = 0; i < 2 * pairs; i++) {
                struct conn *c = &b->conns[i];
                if (!c->failed && c->wlen > c->woff) {
                    conn_flush(b, c);
                    any_pending = true;
                }
                if (qos == 1 && c->is_pub && !c->failed && c->inflight > 0) {
                    any_pending = true;
                }
            }
            if (qos == 1 && !any_pending && now >= end + 150 * 1000000ULL) {
                bool all_acked_delivered = true;
                for (uint64_t i = 0; i < next; i++) {
                    if (b->recs[i].ack_ns && b->recs[i].ack_reason < 128 && !b->recs[i].recv_ns) {
                        all_acked_delivered = false;
                        break;
                    }
                }
                if (all_acked_delivered) break;
            }
        }
        int timeout_ms = 0;
        now = now_ns();
        if (next < count && now < end) {
            if (b->recs[next].scheduled_ns > now + 1000000ULL) timeout_ms = 1;
        } else {
            timeout_ms = 2;
        }
        poll_once(b, timeout_ms);
    }

    uint64_t submitted = 0, acked_ok = 0, acked_refused = 0, ack_missing = 0;
    uint64_t received = 0, in_window = 0, duplicates = 0, ack_no_deliv = 0;
    uint64_t *deliv_lat = malloc(count * sizeof(uint64_t));
    uint64_t *ack_lat = malloc(count * sizeof(uint64_t));
    size_t deliv_n = 0, ack_n = 0;
    for (uint64_t i = 0; i < count; i++) {
        struct msg_rec *r = &b->recs[i];
        if (r->submitted_ns) {
            submitted++;
            if (qos == 1) {
                if (r->ack_ns) {
                    if (r->ack_reason < 128) {
                        acked_ok++;
                        ack_lat[ack_n++] = r->ack_ns - r->submitted_ns;
                        if (!r->recv_ns) ack_no_deliv++;
                    } else {
                        acked_refused++;
                    }
                } else {
                    ack_missing++;
                }
            }
        }
        if (r->recv_ns) {
            received++;
            if (r->recv_ns <= end) in_window++;
            if (r->submitted_ns && r->recv_ns >= r->submitted_ns) {
                deliv_lat[deliv_n++] = r->recv_ns - r->submitted_ns;
            }
            if (r->copies > 1) duplicates += r->copies - 1;
        }
    }
    qsort(deliv_lat, deliv_n, sizeof(uint64_t), cmp_u64);
    qsort(ack_lat, ack_n, sizeof(uint64_t), cmp_u64);
    double dur_s = (double)duration_ms / 1000.0;
    double d_p50 = deliv_n ? (double)deliv_lat[(deliv_n * 50 - 1) / 100] / 1e6 : 0.0;
    double d_p95 = deliv_n ? (double)deliv_lat[(deliv_n * 95 - 1) / 100] / 1e6 : 0.0;
    double d_p99 = deliv_n ? (double)deliv_lat[(deliv_n * 99 - 1) / 100] / 1e6 : 0.0;
    double a_p50 = ack_n ? (double)ack_lat[(ack_n * 50 - 1) / 100] / 1e6 : 0.0;
    double a_p99 = ack_n ? (double)ack_lat[(ack_n * 99 - 1) / 100] / 1e6 : 0.0;

    printf("{\"pairs\":%u,\"qos\":%u,\"rate\":%u,\"tls\":%s,\"scheduled\":%" PRIu64
           ",\"submitted\":%" PRIu64 ",\"acked_ok\":%" PRIu64 ",\"acked_refused\":%" PRIu64
           ",\"ack_missing\":%" PRIu64 ",\"received\":%" PRIu64 ",\"delivered_after_drain\":%" PRIu64
           ",\"in_window\":%" PRIu64 ",\"delivered_mps\":%.1f,\"wire_mps\":%.1f,\"duplicates\":%" PRIu64
           ",\"ack_no_deliv\":%" PRIu64 ",\"invalid\":%" PRIu64 ",\"disconnects\":%" PRIu64
           ",\"deliv_ms\":[%.2f,%.2f,%.2f],\"ack_ms\":[%.2f,%.2f]}\n",
           pairs, qos, rate, use_tls ? "true" : "false", count,
           submitted, acked_ok, acked_refused, ack_missing, received, received, in_window,
           (double)received / dur_s, (double)in_window / dur_s, duplicates,
           ack_no_deliv, b->invalid, b->disconnects, d_p50, d_p95, d_p99, a_p50, a_p99);

    for (unsigned i = 0; i < 2 * pairs; i++) {
        if (b->conns[i].fd >= 0) {
            uint8_t disc[2] = {0xe0, 0};
            conn_append(&b->conns[i], disc, 2);
            conn_flush(b, &b->conns[i]);
        }
    }
    return 0;
}
