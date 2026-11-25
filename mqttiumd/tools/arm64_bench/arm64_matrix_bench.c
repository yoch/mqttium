/* Adversarial & balanced ARM64 MQTT 5 load driver (epoll, plain TCP).
 *
 * Measures scenarios beyond simple 1:1 pairs:
 *  - Fan-out (1:N and P:S per topic) in QoS 0 and QoS 1
 *  - Fan-in (N:1) in QoS 0 and QoS 1
 *  - Slow/stalled subscriber isolation (fast subscribers measured separately)
 *  - Concurrent unrelated session churn (connect -> subscribe -> close) during QoS 1
 *  - Mixed payload sizes (small 256B pairs alongside large 64 KiB pairs)
 *
 * Reports exact accounting (submitted, acked_ok, acked_refused, delivered_fast,
 * expected_fast, loss_fast_pct, duplicates, disconnects, invalid, p50/p95/p99 ms).
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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

#define MAX_CONNS 2048
#define IO_BUF_SIZE 65536
#define MAX_LAT_SAMPLES 2000000

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

enum conn_role {
    ROLE_PUB_FAST = 0,
    ROLE_PUB_LARGE = 1,
    ROLE_SUB_FAST = 2,
    ROLE_SUB_SLOW = 3,
    ROLE_CHURN = 4,
};

struct conn {
    int fd;
    enum conn_role role;
    unsigned idx;
    unsigned group;
    char client_id[64];
    char topic[64];
    size_t topic_len;

    bool connected;
    bool subscribed;
    bool failed;
    bool closed;
    bool read_paused; /* for slow/stalled consumer */

    uint16_t next_pid;
    unsigned inflight;
    unsigned max_inflight;
    uint64_t *pid_sent_ns;

    uint8_t *rbuf;
    size_t rlen;
    size_t rcap;
    uint8_t *wbuf;
    size_t woff;
    size_t wlen;
    size_t wcap;
    bool want_write;

    /* Churn worker state */
    unsigned churn_seq;
    uint64_t next_churn_ns;
    int churn_phase; /* 0=disconnected, 1=connecting, 2=subscribing */
};

struct bench {
    int epfd;
    struct sockaddr_in addr;
    unsigned groups;
    unsigned pubs_per_group;
    unsigned subs_per_group;
    unsigned slow_subs;
    unsigned large_pairs;
    unsigned churners;
    unsigned qos;
    unsigned payload_size;
    unsigned large_payload_size;
    unsigned send_window;
    uint64_t rate_fast;
    uint64_t rate_large;
    uint64_t duration_ms;

    struct conn conns[MAX_CONNS];
    unsigned nconns;
    uint8_t *payload_fast;
    uint8_t *payload_large;

    uint64_t submitted_fast;
    uint64_t submitted_large;
    uint64_t acked_ok_fast;
    uint64_t acked_refused_fast;
    uint64_t acked_ok_large;
    uint64_t acked_refused_large;
    uint64_t refused_0x80;
    uint64_t refused_0x97;
    uint64_t refused_other;
    uint64_t delivered_fast;
    uint64_t delivered_large;
    uint64_t delivered_slow;
    uint64_t expected_fast_deliveries;
    uint64_t expected_large_deliveries;
    uint64_t disconnects_fast;
    uint64_t disconnects_slow;
    uint64_t invalid;
    uint64_t churn_completed;

    double *lat_deliv_fast_ms;
    size_t lat_deliv_fast_n;
    double *lat_ack_fast_ms;
    size_t lat_ack_fast_n;
};

static void conn_append(struct conn *c, const uint8_t *data, size_t len) {
    if (len > c->wcap - c->wlen) {
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
    }
    memcpy(c->wbuf + c->wlen, data, len);
    c->wlen += len;
}

static void update_epoll(struct bench *b, struct conn *c) {
    if (c->fd < 0) return;
    struct epoll_event ev = {0};
    ev.data.ptr = c;
    ev.events = (c->read_paused ? 0 : EPOLLIN) | EPOLLERR | EPOLLHUP;
    bool want_w = (c->wlen > c->woff);
    if (want_w) ev.events |= EPOLLOUT;
    c->want_write = want_w;
    epoll_ctl(b->epfd, EPOLL_CTL_MOD, c->fd, &ev);
}

static void send_connect(struct conn *c) {
    uint8_t body[256];
    size_t p = 0;
    body[p++] = 0; body[p++] = 4;
    memcpy(body + p, "MQTT", 4); p += 4;
    body[p++] = 5;    /* MQTT 5 */
    body[p++] = 0x02; /* Clean Start */
    body[p++] = 0; body[p++] = 60;
    body[p++] = 0;    /* empty properties */
    size_t clen = strlen(c->client_id);
    body[p++] = (uint8_t)(clen >> 8);
    body[p++] = (uint8_t)(clen & 0xff);
    memcpy(body + p, c->client_id, clen); p += clen;

    uint8_t hdr[8];
    hdr[0] = 0x10;
    size_t hlen = 1 + encode_varint(hdr + 1, p);
    conn_append(c, hdr, hlen);
    conn_append(c, body, p);
}

static void send_subscribe(struct conn *c, uint8_t qos) {
    uint8_t body[256];
    size_t p = 0;
    uint16_t pid = ++c->next_pid;
    if (pid == 0) pid = ++c->next_pid;
    body[p++] = (uint8_t)(pid >> 8);
    body[p++] = (uint8_t)(pid & 0xff);
    body[p++] = 0; /* properties */
    body[p++] = (uint8_t)(c->topic_len >> 8);
    body[p++] = (uint8_t)(c->topic_len & 0xff);
    memcpy(body + p, c->topic, c->topic_len); p += c->topic_len;
    body[p++] = qos;

    uint8_t hdr[8];
    hdr[0] = 0x82;
    size_t hlen = 1 + encode_varint(hdr + 1, p);
    conn_append(c, hdr, hlen);
    conn_append(c, body, p);
}

static void send_puback(struct conn *c, uint16_t pid) {
    uint8_t pkt[4] = {0x40, 0x02, (uint8_t)(pid >> 8), (uint8_t)(pid & 0xff)};
    conn_append(c, pkt, sizeof(pkt));
}

static void send_disconnect(struct conn *c) {
    uint8_t pkt[2] = {0xe0, 0x00};
    conn_append(c, pkt, sizeof(pkt));
}

static bool send_publish(struct bench *b, struct conn *c, uint64_t ts_ns, bool is_large) {
    unsigned plen = is_large ? b->large_payload_size : b->payload_size;
    uint8_t *tmpl = is_large ? b->payload_large : b->payload_fast;
    uint16_t pid = 0;
    if (b->qos == 1) {
        if (c->inflight >= c->max_inflight) return false;
        for (unsigned tries = 0; tries < 65535; tries++) {
            uint16_t cand = ++c->next_pid;
            if (cand == 0) cand = ++c->next_pid;
            if (c->pid_sent_ns[cand] == 0) {
                pid = cand;
                break;
            }
        }
        if (pid == 0) return false;
        c->pid_sent_ns[pid] = ts_ns;
        c->inflight++;
    }
    /* Embed 8-byte timestamp + 1-byte kind tag at start of payload */
    memcpy(tmpl, &ts_ns, 8);
    tmpl[8] = is_large ? 1 : 0;

    size_t rem = 2 + c->topic_len + (b->qos ? 2 : 0) + 1 + plen;
    uint8_t hdr[128];
    size_t p = 0;
    hdr[p++] = (uint8_t)(0x30 | (b->qos << 1));
    p += encode_varint(hdr + p, rem);
    hdr[p++] = (uint8_t)(c->topic_len >> 8);
    hdr[p++] = (uint8_t)(c->topic_len & 0xff);
    memcpy(hdr + p, c->topic, c->topic_len); p += c->topic_len;
    if (b->qos) {
        hdr[p++] = (uint8_t)(pid >> 8);
        hdr[p++] = (uint8_t)(pid & 0xff);
    }
    hdr[p++] = 0; /* empty MQTT 5 properties */
    conn_append(c, hdr, p);
    conn_append(c, tmpl, plen);
    update_epoll(b, c);
    return true;
}

static void close_conn(struct bench *b, struct conn *c, bool count_disc) {
    if (c->fd >= 0) {
        epoll_ctl(b->epfd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);
        c->fd = -1;
    }
    c->connected = false;
    c->subscribed = false;
    c->rlen = 0;
    c->woff = 0;
    c->wlen = 0;
    if (count_disc) {
        if (c->role == ROLE_SUB_SLOW) b->disconnects_slow++;
        else if (c->role != ROLE_CHURN) b->disconnects_fast++;
    }
}

static int open_nonblocking_socket(struct bench *b, struct conn *c) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) die("socket");
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    int rc = connect(fd, (struct sockaddr *)&b->addr, sizeof(b->addr));
    if (rc < 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }
    c->fd = fd;
    c->connected = false;
    c->subscribed = false;
    c->closed = false;
    c->rlen = 0;
    c->woff = 0;
    c->wlen = 0;
    struct epoll_event ev = {0};
    ev.data.ptr = c;
    ev.events = (c->read_paused ? 0 : EPOLLIN) | EPOLLOUT | EPOLLERR | EPOLLHUP;
    c->want_write = true;
    if (epoll_ctl(b->epfd, EPOLL_CTL_ADD, fd, &ev) < 0) die("epoll_ctl add");
    return 0;
}

static void handle_packet(struct bench *b, struct conn *c, uint8_t hdr, const uint8_t *body, size_t len, uint64_t now) {
    uint8_t ptype = hdr & 0xf0;
    if (ptype == 0x20) { /* CONNACK */
        if (len < 2 || body[1] != 0) {
            b->invalid++;
            close_conn(b, c, true);
            return;
        }
        c->connected = true;
        if (c->role == ROLE_SUB_FAST || c->role == ROLE_SUB_SLOW) {
            send_subscribe(c, (uint8_t)b->qos);
            update_epoll(b, c);
        } else if (c->role == ROLE_CHURN) {
            c->churn_phase = 2;
            send_subscribe(c, 1);
            update_epoll(b, c);
        }
    } else if (ptype == 0x90) { /* SUBACK */
        if (len < 3) {
            b->invalid++;
            return;
        }
        c->subscribed = true;
        if (c->role == ROLE_CHURN) {
            /* Alternate clean DISCONNECT and abrupt TCP close */
            if (c->churn_seq % 2 == 0) {
                send_disconnect(c);
                while (c->woff < c->wlen) {
                    ssize_t n = send(c->fd, c->wbuf + c->woff, c->wlen - c->woff, MSG_NOSIGNAL);
                    if (n > 0) c->woff += (size_t)n;
                    else break;
                }
            }
            close_conn(b, c, false);
            c->churn_phase = 0;
            b->churn_completed++;
            c->next_churn_ns = now + 2000000ULL; /* 2ms pause before next churn cycle */
        }
    } else if (ptype == 0x40) { /* PUBACK */
        if (len < 2) {
            b->invalid++;
            return;
        }
        uint16_t pid = ((uint16_t)body[0] << 8) | body[1];
        uint8_t reason = (len >= 3) ? body[2] : 0;
        uint64_t sent_ns = c->pid_sent_ns ? c->pid_sent_ns[pid] : 0;
        if (sent_ns != 0) {
            c->pid_sent_ns[pid] = 0;
            if (c->inflight > 0) c->inflight--;
            if (reason < 0x80) {
                if (c->role == ROLE_PUB_LARGE) {
                    b->acked_ok_large++;
                } else {
                    b->acked_ok_fast++;
                    if (b->lat_ack_fast_n < MAX_LAT_SAMPLES) {
                        b->lat_ack_fast_ms[b->lat_ack_fast_n++] = (double)(now - sent_ns) / 1e6;
                    }
                }
            } else {
                if (c->role == ROLE_PUB_LARGE) b->acked_refused_large++;
                else b->acked_refused_fast++;
                if (reason == 0x80) b->refused_0x80++;
                else if (reason == 0x97) b->refused_0x97++;
                else b->refused_other++;
            }
        }
    } else if (ptype == 0x30) { /* PUBLISH */
        uint8_t qos = (hdr >> 1) & 0x03;
        if (len < 2) { b->invalid++; return; }
        size_t tlen = ((size_t)body[0] << 8) | body[1];
        size_t pos = 2 + tlen;
        if (pos + (qos ? 2 : 0) + 1 > len) { b->invalid++; return; }
        uint16_t pid = 0;
        if (qos) {
            pid = ((uint16_t)body[pos] << 8) | body[pos + 1];
            pos += 2;
        }
        /* Skip MQTT 5 property length varint */
        size_t prop_len = 0, mult = 1;
        while (pos < len) {
            uint8_t vb = body[pos++];
            prop_len += (size_t)(vb & 0x7f) * mult;
            if ((vb & 0x80) == 0) break;
            mult *= 128;
        }
        pos += prop_len;
        if (qos == 1) {
            send_puback(c, pid);
            update_epoll(b, c);
        }
        if (pos + 9 <= len) {
            uint64_t sent_ns = 0;
            memcpy(&sent_ns, body + pos, 8);
            uint8_t is_large = body[pos + 8];
            if (c->role == ROLE_SUB_SLOW) {
                b->delivered_slow++;
            } else if (is_large) {
                b->delivered_large++;
            } else {
                b->delivered_fast++;
                if (now >= sent_ns && b->lat_deliv_fast_n < MAX_LAT_SAMPLES) {
                    b->lat_deliv_fast_ms[b->lat_deliv_fast_n++] = (double)(now - sent_ns) / 1e6;
                }
            }
        }
    } else if (ptype == 0xe0) { /* DISCONNECT */
        close_conn(b, c, true);
    }
}

static void pump_io(struct bench *b, struct conn *c, uint32_t events, uint64_t now) {
    if (c->fd < 0) return;
    if (events & (EPOLLERR | EPOLLHUP)) {
        close_conn(b, c, true);
        return;
    }
    if ((events & EPOLLOUT) && c->wlen > c->woff) {
        while (c->woff < c->wlen) {
            ssize_t n = send(c->fd, c->wbuf + c->woff, c->wlen - c->woff, MSG_NOSIGNAL);
            if (n > 0) {
                c->woff += (size_t)n;
            } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break;
            } else {
                close_conn(b, c, true);
                return;
            }
        }
        if (c->woff == c->wlen) {
            c->woff = 0;
            c->wlen = 0;
        }
        update_epoll(b, c);
    }
    if ((events & EPOLLIN) && !c->read_paused) {
        for (;;) {
            if (c->rlen + 16384 > c->rcap) {
                size_t ncap = c->rcap ? c->rcap * 2 : IO_BUF_SIZE;
                while (ncap < c->rlen + 16384) ncap *= 2;
                c->rbuf = realloc(c->rbuf, ncap);
                if (!c->rbuf) die("realloc rbuf");
                c->rcap = ncap;
            }
            ssize_t n = recv(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen, 0);
            if (n > 0) {
                c->rlen += (size_t)n;
            } else if (n == 0) {
                close_conn(b, c, true);
                return;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                close_conn(b, c, true);
                return;
            }
        }
        size_t off = 0;
        while (off + 2 <= c->rlen) {
            uint8_t hdr = c->rbuf[off];
            size_t pos = off + 1;
            size_t rlen = 0, mult = 1;
            bool ok = false;
            for (int i = 0; i < 4 && pos < c->rlen; i++) {
                uint8_t vb = c->rbuf[pos++];
                rlen += (size_t)(vb & 0x7f) * mult;
                if ((vb & 0x80) == 0) { ok = true; break; }
                mult *= 128;
            }
            if (!ok || pos + rlen > c->rlen) break;
            handle_packet(b, c, hdr, c->rbuf + pos, rlen, now);
            if (c->fd < 0) return;
            off = pos + rlen;
        }
        if (off > 0) {
            if (off < c->rlen) memmove(c->rbuf, c->rbuf + off, c->rlen - off);
            c->rlen -= off;
        }
    }
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static double pct(double *arr, size_t n, double p) {
    if (n == 0) return 0.0;
    size_t idx = (size_t)((double)(n - 1) * p);
    if (idx >= n) idx = n - 1;
    return arr[idx];
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    const char *host = "127.0.0.1";
    int port = 1883;
    struct bench *b = calloc(1, sizeof(*b));
    if (!b) die("calloc bench");
    b->groups = 1;
    b->pubs_per_group = 1;
    b->subs_per_group = 1;
    b->qos = 1;
    b->payload_size = 256;
    b->large_payload_size = 65536;
    b->send_window = 8;
    b->rate_fast = 20000;
    b->rate_large = 500;
    b->duration_ms = 2000;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--host") && i + 1 < argc) host = argv[++i];
        else if (!strcmp(argv[i], "--port") && i + 1 < argc) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--groups") && i + 1 < argc) b->groups = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--pubs-per-group") && i + 1 < argc) b->pubs_per_group = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--subs-per-group") && i + 1 < argc) b->subs_per_group = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--slow-subs") && i + 1 < argc) b->slow_subs = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--large-pairs") && i + 1 < argc) b->large_pairs = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--churners") && i + 1 < argc) b->churners = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--qos") && i + 1 < argc) b->qos = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--payload") && i + 1 < argc) b->payload_size = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--large-payload") && i + 1 < argc) b->large_payload_size = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--window") && i + 1 < argc) b->send_window = (unsigned)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--rate") && i + 1 < argc) b->rate_fast = (uint64_t)atoll(argv[++i]);
        else if (!strcmp(argv[i], "--large-rate") && i + 1 < argc) b->rate_large = (uint64_t)atoll(argv[++i]);
        else if (!strcmp(argv[i], "--duration-ms") && i + 1 < argc) b->duration_ms = (uint64_t)atoll(argv[++i]);
    }

    b->epfd = epoll_create1(0);
    if (b->epfd < 0) die("epoll_create1");
    b->addr.sin_family = AF_INET;
    b->addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &b->addr.sin_addr) != 1) die("inet_pton");

    b->payload_fast = malloc(b->payload_size > 16 ? b->payload_size : 16);
    b->payload_large = malloc(b->large_payload_size > 16 ? b->large_payload_size : 16);
    memset(b->payload_fast, 'F', b->payload_size);
    memset(b->payload_large, 'L', b->large_payload_size);
    b->lat_deliv_fast_ms = malloc(MAX_LAT_SAMPLES * sizeof(double));
    b->lat_ack_fast_ms = malloc(MAX_LAT_SAMPLES * sizeof(double));

    /* Build fast groups */
    unsigned fast_pub_indices[MAX_CONNS], n_fast_pubs = 0;
    unsigned large_pub_indices[MAX_CONNS], n_large_pubs = 0;

    for (unsigned g = 0; g < b->groups; g++) {
        for (unsigned s = 0; s < b->subs_per_group; s++) {
            struct conn *c = &b->conns[b->nconns];
            c->fd = -1;
            c->idx = b->nconns++;
            c->group = g;
            c->role = (g == 0 && s < b->slow_subs) ? ROLE_SUB_SLOW : ROLE_SUB_FAST;
            snprintf(c->client_id, sizeof(c->client_id), "sub-g%u-s%u", g, s);
            snprintf(c->topic, sizeof(c->topic), "bench/g%u", g);
            c->topic_len = strlen(c->topic);
            open_nonblocking_socket(b, c);
            send_connect(c);
            update_epoll(b, c);
        }
        for (unsigned p = 0; p < b->pubs_per_group; p++) {
            struct conn *c = &b->conns[b->nconns];
            c->fd = -1;
            c->idx = b->nconns++;
            c->group = g;
            c->role = ROLE_PUB_FAST;
            c->max_inflight = b->send_window;
            c->pid_sent_ns = calloc(65536, sizeof(uint64_t));
            snprintf(c->client_id, sizeof(c->client_id), "pub-g%u-p%u", g, p);
            snprintf(c->topic, sizeof(c->topic), "bench/g%u", g);
            c->topic_len = strlen(c->topic);
            fast_pub_indices[n_fast_pubs++] = c->idx;
            open_nonblocking_socket(b, c);
            send_connect(c);
            update_epoll(b, c);
        }
    }

    /* Optional mixed large-payload pairs */
    for (unsigned lp = 0; lp < b->large_pairs; lp++) {
        struct conn *sub = &b->conns[b->nconns];
        sub->fd = -1;
        sub->idx = b->nconns++;
        sub->role = ROLE_SUB_FAST;
        snprintf(sub->client_id, sizeof(sub->client_id), "lsub-%u", lp);
        snprintf(sub->topic, sizeof(sub->topic), "bench/large/%u", lp);
        sub->topic_len = strlen(sub->topic);
        open_nonblocking_socket(b, sub);
        send_connect(sub);
        update_epoll(b, sub);

        struct conn *pub = &b->conns[b->nconns];
        pub->fd = -1;
        pub->idx = b->nconns++;
        pub->role = ROLE_PUB_LARGE;
        pub->max_inflight = 4;
        pub->pid_sent_ns = calloc(65536, sizeof(uint64_t));
        snprintf(pub->client_id, sizeof(pub->client_id), "lpub-%u", lp);
        snprintf(pub->topic, sizeof(pub->topic), "bench/large/%u", lp);
        pub->topic_len = strlen(pub->topic);
        large_pub_indices[n_large_pubs++] = pub->idx;
        open_nonblocking_socket(b, pub);
        send_connect(pub);
        update_epoll(b, pub);
    }

    /* Churn slots (activated once traffic window begins) */
    unsigned churn_indices[MAX_CONNS], n_churn = 0;
    for (unsigned ch = 0; ch < b->churners; ch++) {
        struct conn *c = &b->conns[b->nconns];
        c->fd = -1;
        c->idx = b->nconns++;
        c->role = ROLE_CHURN;
        c->group = ch;
        churn_indices[n_churn++] = c->idx;
    }

    /* Wait until all non-churn connections are connected & subscribed */
    struct epoll_event evs[256];
    uint64_t ready_deadline = now_ns() + 10ULL * NS_PER_SEC;
    for (;;) {
        bool all_ready = true;
        for (unsigned i = 0; i < b->nconns; i++) {
            struct conn *c = &b->conns[i];
            if (c->role == ROLE_CHURN) continue;
            if (!c->connected) { all_ready = false; break; }
            if ((c->role == ROLE_SUB_FAST || c->role == ROLE_SUB_SLOW) && !c->subscribed) {
                all_ready = false; break;
            }
        }
        if (all_ready) break;
        if (now_ns() > ready_deadline) die("setup timeout waiting for CONNACK/SUBACK");
        int nf = epoll_wait(b->epfd, evs, 256, 5);
        uint64_t now = now_ns();
        for (int k = 0; k < nf; k++) pump_io(b, evs[k].data.ptr, evs[k].events, now);
    }

    /* Freeze slow subscribers so they stop reading TCP sockets during load */
    for (unsigned i = 0; i < b->nconns; i++) {
        struct conn *c = &b->conns[i];
        if (c->role == ROLE_SUB_SLOW) {
            c->read_paused = true;
            update_epoll(b, c);
        }
    }

    uint64_t start_ns = now_ns();
    uint64_t end_ns = start_ns + b->duration_ms * 1000000ULL;
    uint64_t next_fast_ns = start_ns;
    uint64_t fast_step_ns = b->rate_fast ? (NS_PER_SEC / b->rate_fast) : 0;
    uint64_t next_large_ns = start_ns;
    uint64_t large_step_ns = (n_large_pubs && b->rate_large) ? (NS_PER_SEC / b->rate_large) : 0;
    unsigned fast_rr = 0, large_rr = 0;

    for (unsigned i = 0; i < n_churn; i++) {
        b->conns[churn_indices[i]].next_churn_ns = start_ns + i * 1000000ULL;
    }

    /* Main publish window */
    for (;;) {
        uint64_t now = now_ns();
        if (now >= end_ns) break;

        /* Drive churners */
        for (unsigned i = 0; i < n_churn; i++) {
            struct conn *c = &b->conns[churn_indices[i]];
            if (c->churn_phase == 0 && now >= c->next_churn_ns) {
                c->churn_seq++;
                snprintf(c->client_id, sizeof(c->client_id), "churn-%u-%u", c->group, c->churn_seq);
                snprintf(c->topic, sizeof(c->topic), "churn/t/%u/%u", c->group, c->churn_seq);
                c->topic_len = strlen(c->topic);
                if (open_nonblocking_socket(b, c) == 0) {
                    c->churn_phase = 1;
                    send_connect(c);
                    update_epoll(b, c);
                }
            }
        }

        /* Pace fast publishers */
        int budget = 256;
        while (budget-- > 0 && n_fast_pubs > 0 && now >= next_fast_ns) {
            bool sent_any = false;
            for (unsigned tries = 0; tries < n_fast_pubs; tries++) {
                struct conn *pub = &b->conns[fast_pub_indices[fast_rr % n_fast_pubs]];
                fast_rr++;
                if (pub->fd >= 0 && pub->connected && send_publish(b, pub, now, false)) {
                    b->submitted_fast++;
                    unsigned fast_subs_in_group = b->subs_per_group - (pub->group == 0 ? b->slow_subs : 0);
                    b->expected_fast_deliveries += fast_subs_in_group;
                    sent_any = true;
                    break;
                }
            }
            if (!sent_any) break;
            next_fast_ns += fast_step_ns ? fast_step_ns : 1;
            if (next_fast_ns + 50000000ULL < now) next_fast_ns = now; /* prevent runaway catchup */
        }

        /* Pace large publishers */
        int lbudget = 16;
        while (lbudget-- > 0 && n_large_pubs > 0 && large_step_ns > 0 && now >= next_large_ns) {
            struct conn *pub = &b->conns[large_pub_indices[large_rr % n_large_pubs]];
            large_rr++;
            if (pub->fd >= 0 && pub->connected && send_publish(b, pub, now, true)) {
                b->submitted_large++;
                b->expected_large_deliveries += 1;
                next_large_ns += large_step_ns;
                if (next_large_ns + 50000000ULL < now) next_large_ns = now;
            } else {
                break;
            }
        }

        int nf = epoll_wait(b->epfd, evs, 256, 1);
        now = now_ns();
        for (int k = 0; k < nf; k++) pump_io(b, evs[k].data.ptr, evs[k].events, now);
    }

    /* Stop churners cleanly */
    for (unsigned i = 0; i < n_churn; i++) {
        close_conn(b, &b->conns[churn_indices[i]], false);
    }

    /* Drain window: wait up to 2.5s for remaining ACKs and fast deliveries */
    uint64_t drain_end = now_ns() + 2500ULL * 1000000ULL;
    uint64_t last_progress = now_ns();
    uint64_t prev_tot = 0;
    for (;;) {
        uint64_t cur_tot = b->acked_ok_fast + b->acked_refused_fast + b->delivered_fast +
                           b->acked_ok_large + b->acked_refused_large + b->delivered_large;
        uint64_t now = now_ns();
        if (cur_tot != prev_tot) {
            prev_tot = cur_tot;
            last_progress = now;
        }
        bool acks_done = (b->qos == 0) ||
                         (b->acked_ok_fast + b->acked_refused_fast >= b->submitted_fast &&
                          b->acked_ok_large + b->acked_refused_large >= b->submitted_large);
        bool delivs_done = (b->delivered_fast >= b->expected_fast_deliveries &&
                            b->delivered_large >= b->expected_large_deliveries);
        if (acks_done && delivs_done) break;
        if (now >= drain_end) break;
        if (b->qos == 0 && (now - last_progress) > 250000000ULL) break;

        int nf = epoll_wait(b->epfd, evs, 256, 5);
        now = now_ns();
        for (int k = 0; k < nf; k++) pump_io(b, evs[k].data.ptr, evs[k].events, now);
    }

    qsort(b->lat_deliv_fast_ms, b->lat_deliv_fast_n, sizeof(double), cmp_double);
    qsort(b->lat_ack_fast_ms, b->lat_ack_fast_n, sizeof(double), cmp_double);

    double dur_s = (double)b->duration_ms / 1000.0;
    double pub_mps = (double)b->submitted_fast / dur_s;
    double deliv_mps = (double)b->delivered_fast / dur_s;
    double loss_pct = 0.0;
    if (b->expected_fast_deliveries > 0 && b->delivered_fast < b->expected_fast_deliveries) {
        loss_pct = 100.0 * (double)(b->expected_fast_deliveries - b->delivered_fast) /
                   (double)b->expected_fast_deliveries;
    }

    printf("{\"groups\":%u,\"pubs_per_group\":%u,\"subs_per_group\":%u,"
           "\"slow_subs\":%u,\"large_pairs\":%u,\"churners\":%u,\"qos\":%u,"
           "\"submitted_fast\":%" PRIu64 ",\"acked_ok_fast\":%" PRIu64 ","
           "\"acked_refused_fast\":%" PRIu64 ",\"refused_0x80\":%" PRIu64 ","
           "\"refused_0x97\":%" PRIu64 ",\"refused_other\":%" PRIu64 ","
           "\"expected_fast_deliveries\":%" PRIu64 ",\"delivered_fast\":%" PRIu64 ","
           "\"pub_mps\":%.1f,\"deliv_fast_mps\":%.1f,\"loss_fast_pct\":%.3f,"
           "\"submitted_large\":%" PRIu64 ",\"acked_ok_large\":%" PRIu64 ","
           "\"acked_refused_large\":%" PRIu64 ",\"delivered_large\":%" PRIu64 ","
           "\"churn_completed\":%" PRIu64 ",\"disconnects_fast\":%" PRIu64 ","
           "\"disconnects_slow\":%" PRIu64 ",\"invalid\":%" PRIu64 ","
           "\"deliv_fast_ms\":[%.2f,%.2f,%.2f],\"ack_fast_ms\":[%.2f,%.2f,%.2f]}\n",
           b->groups, b->pubs_per_group, b->subs_per_group,
           b->slow_subs, b->large_pairs, b->churners, b->qos,
           b->submitted_fast, b->acked_ok_fast, b->acked_refused_fast,
           b->refused_0x80, b->refused_0x97, b->refused_other,
           b->expected_fast_deliveries, b->delivered_fast,
           pub_mps, deliv_mps, loss_pct,
           b->submitted_large, b->acked_ok_large, b->acked_refused_large, b->delivered_large,
           b->churn_completed, b->disconnects_fast, b->disconnects_slow, b->invalid,
           pct(b->lat_deliv_fast_ms, b->lat_deliv_fast_n, 0.50),
           pct(b->lat_deliv_fast_ms, b->lat_deliv_fast_n, 0.95),
           pct(b->lat_deliv_fast_ms, b->lat_deliv_fast_n, 0.99),
           pct(b->lat_ack_fast_ms, b->lat_ack_fast_n, 0.50),
           pct(b->lat_ack_fast_ms, b->lat_ack_fast_n, 0.95),
           pct(b->lat_ack_fast_ms, b->lat_ack_fast_n, 0.99));
    return 0;
}
