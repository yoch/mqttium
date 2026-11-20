/* One paced, nonblocking MQTT 5 client process shared by both broker runs. */
#define _GNU_SOURCE
#include <errno.h>
#include <inttypes.h>
#include <mosquitto.h>
#include <mqtt_protocol.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum { MAX_PAIRS = 8, MAX_EVENTS = 2000000, SEND_WINDOW = 128, LOCAL_WINDOW_FULL = -1 };
static const uint64_t SECOND = 1000000000;

struct event {
	uint64_t scheduled, submitted, ack, received;
	unsigned copies;
	int send_rc, ack_reason;
};

struct run {
	struct event *events;
	unsigned count, pairs, qos, payload_size, invalid, stray_ack, disconnects;
	char *payload;
};

struct peer {
	struct run *run;
	struct mosquitto *mqtt;
	char topic[40];
	unsigned pair, pending, disconnects;
	bool publisher, connected, subscribed, failed;
	uint32_t *pending_by_mid;
};

static uint64_t now_ns(void)
{
	struct timespec t;
	if(clock_gettime(CLOCK_MONOTONIC, &t)) {
		perror("clock_gettime");
		exit(2);
	}
	return (uint64_t)t.tv_sec * SECOND + (uint64_t)t.tv_nsec;
}

static void mqtt_ok(int rc, const char *operation)
{
	if(rc) {
		fprintf(stderr, "%s: %s\n", operation, mosquitto_strerror(rc));
		exit(2);
	}
}

static void connected(struct mosquitto *mqtt, void *context, int reason, int flags,
		const mosquitto_property *properties)
{
	(void)flags;
	(void)properties;
	struct peer *peer = context;
	peer->connected = reason == 0;
	peer->failed = !peer->connected;
	if(peer->connected && !peer->publisher) {
		mqtt_ok(mosquitto_subscribe_v5(mqtt, NULL, peer->topic, (int)peer->run->qos, 0, NULL), "subscribe");
	}
}

static void subscribed(struct mosquitto *mqtt, void *context, int mid, int count,
		const int *granted, const mosquitto_property *properties)
{
	(void)mqtt;
	(void)mid;
	(void)properties;
	struct peer *peer = context;
	peer->subscribed = count == 1 && granted[0] == (int)peer->run->qos;
	peer->failed = !peer->subscribed;
}

static void disconnected(struct mosquitto *mqtt, void *context, int reason,
		const mosquitto_property *properties)
{
	(void)mqtt;
	(void)reason;
	(void)properties;
	struct peer *peer = context;
	if(!peer->failed) {
		peer->run->disconnects++;
		peer->disconnects++;
	}
	peer->failed = true;
	peer->connected = false;
}

static void acknowledged(struct mosquitto *mqtt, void *context, int mid, int reason,
		const mosquitto_property *properties)
{
	(void)mqtt;
	(void)properties;
	struct peer *peer = context;
	if(!peer->publisher || !peer->run->qos) {
		return;
	}
	uint32_t index = peer->pending_by_mid[mid];
	if(!index) {
		peer->run->stray_ack++;
		return;
	}
	struct event *event = &peer->run->events[index - 1];
	event->ack = now_ns();
	event->ack_reason = reason;
	peer->pending_by_mid[mid] = 0;
	peer->pending--;
}

static void message(struct mosquitto *mqtt, void *context, const struct mosquitto_message *msg,
		const mosquitto_property *properties)
{
	(void)mqtt;
	(void)properties;
	uint64_t received = now_ns(), id = 0;
	struct peer *peer = context;
	struct run *run = peer->run;
	if(peer->publisher || strcmp(msg->topic, peer->topic) || msg->retain ||
			msg->qos != (int)run->qos || msg->payloadlen != (int)run->payload_size) {
		run->invalid++;
		return;
	}
	memcpy(&id, msg->payload, sizeof(id));
	if(id >= run->count || id % run->pairs != peer->pair || !run->events[id].submitted ||
			memcmp((const char *)msg->payload + sizeof(id), run->payload + sizeof(id),
				run->payload_size - sizeof(id))) {
		run->invalid++;
		return;
	}
	struct event *event = &run->events[id];
	if(event->copies++ == 0) {
		event->received = received;
	}
}

static void connect_peer(struct peer *peer, const char *pki, int port)
{
	const char *role = peer->publisher ? "sensor" : "dashboard";
	char id[40], ca[4096], cert[4096], key[4096];
	snprintf(id, sizeof(id), "%s-%u", role, peer->pair);
	snprintf(peer->topic, sizeof(peer->topic), "atelier/load/%u", peer->pair);
	snprintf(ca, sizeof(ca), "%s/ca.crt", pki);
	snprintf(cert, sizeof(cert), "%s/%s.crt", pki, role);
	snprintf(key, sizeof(key), "%s/%s.key", pki, role);
	peer->mqtt = mosquitto_new(id, peer->run->qos == 0, peer);
	if(!peer->mqtt) {
		perror("mosquitto_new");
		exit(2);
	}
	mqtt_ok(mosquitto_int_option(peer->mqtt, MOSQ_OPT_PROTOCOL_VERSION, MQTT_PROTOCOL_V5), "MQTT 5");
	mqtt_ok(mosquitto_int_option(peer->mqtt, MOSQ_OPT_TCP_NODELAY, 1), "TCP_NODELAY");
	mqtt_ok(mosquitto_int_option(peer->mqtt, MOSQ_OPT_SEND_MAXIMUM, SEND_WINDOW), "send window");
	mqtt_ok(mosquitto_tls_set(peer->mqtt, ca, NULL, cert, key, NULL), "TLS trust");
	mqtt_ok(mosquitto_tls_opts_set(peer->mqtt, 1, "tlsv1.3", NULL), "TLS 1.3");
	mosquitto_connect_v5_callback_set(peer->mqtt, connected);
	mosquitto_disconnect_v5_callback_set(peer->mqtt, disconnected);
	mosquitto_subscribe_v5_callback_set(peer->mqtt, subscribed);
	mosquitto_publish_v5_callback_set(peer->mqtt, acknowledged);
	mosquitto_message_v5_callback_set(peer->mqtt, message);
	mosquitto_property *properties = NULL;
	if(peer->run->qos) {
		mqtt_ok(mosquitto_property_add_int32(&properties, MQTT_PROP_SESSION_EXPIRY_INTERVAL, 3600), "session expiry");
	}
	mqtt_ok(mosquitto_connect_bind_v5(peer->mqtt, "127.0.0.1", port, 30, NULL, properties), "connect");
	mosquitto_property_free_all(&properties);
	uint64_t deadline = now_ns() + 5 * SECOND;
	while(!peer->failed && now_ns() < deadline) {
		if(peer->connected && (peer->publisher || peer->subscribed)) {
			return;
		}
		mqtt_ok(mosquitto_loop(peer->mqtt, 10, 1), "connection or subscription loop");
	}
	fprintf(stderr, "client %s failed to reach CONNACK/SUBACK\n", id);
	exit(2);
}

static void publish(struct peer *peer, unsigned index)
{
	struct run *run = peer->run;
	struct event *event = &run->events[index];
	uint64_t id = index;
	if(peer->failed) {
		event->send_rc = MOSQ_ERR_NO_CONN;
		return;
	}
	if(run->qos && peer->pending >= SEND_WINDOW) {
		event->send_rc = LOCAL_WINDOW_FULL;
		return;
	}
	memcpy(run->payload, &id, sizeof(id));
	int mid = 0;
	event->submitted = now_ns();
	event->send_rc = mosquitto_publish_v5(peer->mqtt, &mid, peer->topic, (int)run->payload_size,
			run->payload, (int)run->qos, false, NULL);
	if(event->send_rc == MOSQ_ERR_SUCCESS && run->qos) {
		if(mid <= 0 || peer->pending_by_mid[mid]) {
			fprintf(stderr, "MID already in flight\n");
			exit(2);
		}
		peer->pending_by_mid[mid] = index + 1;
		peer->pending++;
	}
}

static void sockets_turn(struct peer *peers, unsigned count, uint64_t delay)
{
	struct pollfd sockets[2 * MAX_PAIRS];
	for(unsigned i = 0; i < count; i++) {
		int fd = peers[i].failed ? -1 : mosquitto_socket(peers[i].mqtt);
		sockets[i] = (struct pollfd){fd, POLLIN | (mosquitto_want_write(peers[i].mqtt) ? POLLOUT : 0), 0};
	}
	struct timespec timeout = {.tv_sec = 0, .tv_nsec = (long)delay};
	if(ppoll(sockets, count, &timeout, NULL) < 0 && errno != EINTR) {
		perror("ppoll");
		exit(2);
	}
	for(unsigned i = 0; i < count; i++) {
		if(!sockets[i].revents) {
			continue;
		}
		struct peer *peer = &peers[i];
		int rc = 0;
		if(sockets[i].revents & (POLLIN | POLLHUP | POLLERR)) {
			rc = mosquitto_loop_read(peer->mqtt, 1);
		}
		if(!rc && !peer->failed && (sockets[i].revents & POLLOUT)) {
			rc = mosquitto_loop_write(peer->mqtt, 1);
		}
		if(rc && !peer->failed) {
			disconnected(peer->mqtt, peer, rc, NULL);
		}
	}
}

static unsigned number(const char *arg, unsigned min, unsigned max)
{
	char *end;
	unsigned long n = strtoul(arg, &end, 10);
	if(end == arg || *end || n < min || n > max) {
		fprintf(stderr, "invalid numeric argument: %s\n", arg);
		exit(2);
	}
	return (unsigned)n;
}

int main(int argc, char **argv)
{
	if(signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
		perror("ignoring SIGPIPE");
		return 2;
	}
	if((argc != 9 && argc != 13) || strlen(argv[2]) >= 4000) {
		fprintf(stderr, "usage: load-client PORT PKI PAIRS QOS RATE DURATION_MS PAYLOAD_BYTES CSV"
			" [IDLE_MS DRAIN_MS NEXT_RATE NEXT_DURATION_MS]\n");
		return 2;
	}
	int port = (int)number(argv[1], 1, 65535);
	unsigned rate = number(argv[5], 0, MAX_EVENTS);
	unsigned duration = number(argv[6], 100, 60000);
	unsigned idle_ms = argc == 13 ? number(argv[9], 50, 10000) : 50;
	unsigned drain_ms = argc == 13 ? number(argv[10], 0, 10000) : (rate ? 500 : 0);
	unsigned next_rate = argc == 13 ? number(argv[11], 0, MAX_EVENTS) : 0;
	unsigned next_duration = argc == 13 ? number(argv[12], 0, 60000) : 0;
	if((next_rate == 0) != (next_duration == 0)) {
		fprintf(stderr, "next rate and duration must both be present or both zero\n");
		return 2;
	}
	uint64_t first_count = (uint64_t)rate * duration / 1000;
	uint64_t count = first_count + (uint64_t)next_rate * next_duration / 1000;
	if(count > MAX_EVENTS) {
		fprintf(stderr, "too many scheduled events in one case\n");
		return 2;
	}
	struct run run = {.count = (unsigned)count, .pairs = number(argv[3], 1, MAX_PAIRS),
		.qos = number(argv[4], 0, 1), .payload_size = number(argv[7], 8, 32768)};
	run.events = calloc(run.count ? run.count : 1, sizeof(*run.events));
	run.payload = malloc(run.payload_size);
	if(!run.events || !run.payload) {
		perror("allocating event records");
		return 2;
	}
	memset(run.payload, 'P', run.payload_size);
	mqtt_ok(mosquitto_lib_init(), "client library initialization");
	struct peer peers[2 * MAX_PAIRS] = {0};
	for(unsigned i = 0; i < 2 * run.pairs; i++) {
		struct peer *peer = &peers[i];
		*peer = (struct peer){.run = &run, .pair = i % run.pairs, .publisher = i >= run.pairs};
		if(run.qos && peer->publisher) {
			peer->pending_by_mid = calloc(65536, sizeof(uint32_t));
			if(!peer->pending_by_mid) {
				perror("allocating MID table");
				return 2;
			}
		}
		connect_peer(peer, argv[2], port);
	}

	uint64_t idle_start = now_ns();
	uint64_t start = idle_start + (uint64_t)idle_ms * 1000000;
	uint64_t transition = start + (uint64_t)duration * 1000000;
	uint64_t end = transition + (uint64_t)next_duration * 1000000;
	uint64_t stop = end + (uint64_t)drain_ms * 1000000;
	for(unsigned i = 0; i < run.count; i++) {
		run.events[i].scheduled = i < first_count
			? start + (uint64_t)i * SECOND / rate
			: transition + (i - first_count) * SECOND / next_rate;
		run.events[i].ack_reason = -1;
	}
	printf("{\"idle_start_ns\":%" PRIu64 ",\"start_ns\":%" PRIu64 ",\"switch_ns\":%" PRIu64
		",\"end_ns\":%" PRIu64 ",\"stop_ns\":%" PRIu64 "}\n", idle_start, start, transition, end, stop);
	fflush(stdout);
	unsigned next = 0, attempted = 0;
	uint64_t misc = start;
	for(uint64_t now = now_ns(); now < stop; now = now_ns()) {
		// An offered slot that missed its phase stays visible as unsent.
		if(now >= transition && next < first_count) {
			next = (unsigned)first_count;
		}
		for(unsigned batch = 0; batch < 32 && next < run.count && run.events[next].scheduled <= now && now < end; batch++) {
			publish(&peers[run.pairs + next % run.pairs], next);
			next++;
			attempted++;
		}
		if(now >= misc) {
			for(unsigned i = 0; i < run.pairs * 2; i++) {
				if(!peers[i].failed && mosquitto_loop_misc(peers[i].mqtt)) {
					disconnected(peers[i].mqtt, &peers[i], 0, NULL);
				}
			}
			misc = now + SECOND;
		}
		now = now_ns();
		uint64_t until = now < end && next < run.count ? run.events[next].scheduled : stop;
		uint64_t delay = until > now ? until - now : 0;
		sockets_turn(peers, run.pairs * 2, delay > 20000000 ? 20000000 : delay);
	}

	FILE *csv = fopen(argv[8], "wx");
	if(!csv) {
		perror("opening observations");
		return 2;
	}
	fputs("id,pair,scheduled_ns,submitted_ns,send_rc,ack_ns,ack_reason,received_ns,copies\n", csv);
	for(unsigned i = 0; i < run.count; i++) {
		struct event *e = &run.events[i];
		fprintf(csv, "%u,%u,%" PRIu64 ",%" PRIu64 ",%d,%" PRIu64 ",%d,%" PRIu64 ",%u\n", i,
				i % run.pairs, e->scheduled, e->submitted, e->send_rc, e->ack, e->ack_reason, e->received, e->copies);
	}
	if(fclose(csv)) {
		perror("closing observations");
		return 2;
	}
	printf("{\"attempted_schedule_slots\":%u,\"invalid\":%u,\"unmapped_ack\":%u,\"disconnects\":%u,\"disconnects_by_pair\":[",
			attempted, run.invalid, run.stray_ack, run.disconnects);
	for(unsigned i = 0; i < run.pairs; i++) {
		printf("%s{\"subscriber\":%u,\"publisher\":%u}", i ? "," : "",
				peers[i].disconnects, peers[run.pairs + i].disconnects);
	}
	puts("]}");
	for(unsigned i = 0; i < 2 * run.pairs; i++) {
		mosquitto_disconnect(peers[i].mqtt);
		mosquitto_destroy(peers[i].mqtt);
		free(peers[i].pending_by_mid);
	}
	mosquitto_lib_cleanup();
	free(run.payload);
	free(run.events);
	return 0;
}
