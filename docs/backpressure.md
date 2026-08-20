# Writer backpressure and burst sizing

`AsyncClient` bounds encoded outbound work with two independent writer limits:
`max_outbound_messages` and `max_outbound_bytes`. The message limit controls the
number of writer-resident frames. The byte limit controls how much encoded FIFO
work may remain owned by the writer at once, including the writer's active
batch.

The default `max_outbound_bytes` is **1 MiB**. Treat it as a latency and batching
control as well as a memory bound.

## The byte budget is a FIFO burst reservoir

MQTTium preserves outbound effect order. Once an encoded frame has been admitted
to the writer, a later frame cannot overtake it. This is the property that keeps
wire order predictable, but it also means that queue depth has a latency cost.

If a small publication is admitted after several MiB of earlier writer-resident
traffic, its completion includes the time needed for that older FIFO work to
progress. Raising `max_outbound_bytes` therefore allows a larger burst reservoir
in front of later traffic. The effect is most visible when a large synchronous
producer burst is followed by small latency-sensitive work.

Conversely, lowering the byte budget returns backpressure to producers sooner.
That bounds the amount of earlier writer work that can accumulate, but it may
increase writer batches and producer suspensions. There is no universally best
large value: the correct setting is a workload trade-off between burst
absorption, batching, memory and queue-residence latency.

`max_outbound_messages` is not a substitute for the byte bound when payload
sizes vary. A few large frames can consume much more queue residence and memory
than many small telemetry frames.

## Why the default should not be raised casually

A `FlowControlError`, an enqueue suspension or a full writer byte budget is
backpressure doing its job. Increasing `max_outbound_bytes` merely to make that
signal disappear moves more work into the FIFO reservoir; it does not make the
network or broker faster.

Raise the bound only when all of these are true:

- the service has a concrete burst that should be absorbed locally rather than
  slowed or shed;
- the additional retained encoded bytes fit comfortably inside the process
  memory budget;
- the latency objective tolerates later frames waiting behind that larger burst;
- same-host measurements show a useful throughput or producer-progress benefit
  for the real payload distribution;
- the measurement includes writer queue statistics, not only application
  submission rate.

For latency-sensitive mixed traffic, prefer keeping the bound close to the
smallest value that absorbs the intended burst. The 1 MiB default is a
conservative general-purpose starting point, not a throughput target and not a
claim that every workload should keep exactly that value.

## Oversized frames still make progress

The byte limit is a capacity bound, not a maximum MQTT packet size. An otherwise
empty writer admits one encoded item larger than `max_outbound_bytes` so a legal
large packet cannot deadlock merely because it exceeds the configured queue
budget. No second item is admitted until capacity is released.

Large PUBLISH payloads may also use MQTTium's segmented write representation.
That copy-avoidance policy is independent of the writer byte budget. Do not infer
from `max_outbound_bytes` that payloads above the bound are rejected.

## Observe the writer before changing limits

Use `client.stats().writer` together with transport and protocol state. Useful
fields include:

- `queued_bytes` and `high_water_bytes` for current and peak charged encoded
  bytes owned by the writer; `queued_bytes` remains charged while a batch is
  active;
- `queued_messages` and `high_water_messages` for the live asyncio queue;
- `batches`, `batched_items` and `batched_bytes` for batching behaviour;
- `enqueue_suspensions` and `waiters` for producer-side writer pressure;
- transport `pending_write_bytes` when the transport can expose its own buffered
  output.

`queued_messages` is intentionally the live queue size, not the full resident
admission count. A writer batch that has already been extracted from the queue
still consumes writer capacity until it completes. For operational diagnosis,
read several snapshots over time and combine message and byte fields rather
than treating one instantaneous queue size as the whole writer state.

A useful sizing exercise is to sweep `max_outbound_bytes` over a small range on
one stable host while keeping the workload, broker, payload distribution and
other bounds fixed. Record completed throughput, CPU, queue high water, writer
batch count and latency together. A setting that wins only by moving more bytes
into the queue is not automatically an improvement.

## Mixed-load latency: separate inherited backlog from clear-path latency

Network completion latency starts before the application calls `publish()`, so
it correctly includes local admission and queue residence. That makes the
writer state at sample start part of the workload.

For a mixed-load benchmark, classify samples by whether older writer work already
exists when the sample begins:

- **backlogged sample** — earlier writer-resident work exists. Its latency
  intentionally measures FIFO head-of-line delay plus transport, broker and
  completion time;
- **clear-path sample** — no earlier writer-resident work exists. It measures the
  publish/completion path without inherited writer queue residence.

Both are valid service behaviours, but they answer different questions. Report
them separately when a workload can alternate between the two states.

For supported application-level instrumentation, snapshot
`client.stats().writer.queued_bytes` immediately before the operation being
measured. A non-zero value means earlier encoded work is still charged to the
writer, including work in an active extracted batch. Repo-local diagnostic
harnesses may additionally inspect exact internal resident state when they need
to attribute queue ownership more precisely.

In particular, do not start a large synchronous flood and then describe the
first small probe's percentile as generic steady-state latency without stating
that the probe began behind the flood. A burst-start head-of-line measurement is
useful evidence, but it is a burst-start metric.

Prefer state-based classification over discarding a fixed number of warm-up
probes. Backlog duration depends on payload size, configured writer bounds,
broker speed and host scheduling; "drop the first two samples" can be correct
for one cell and wrong for another.

Public applications should use supported snapshots and their own workload
boundaries rather than depending on private attributes.

## Benchmark identity includes writer bounds

A latency or throughput result is incomplete unless it records
`max_outbound_bytes` and `max_outbound_messages`. Changing either can change:

- when producers encounter backpressure;
- how much FIFO work can precede a later frame;
- writer batch shape;
- memory retained by encoded outbound work;
- the operating point at which transport or broker buffering becomes dominant.

For A/B optimisation work, keep the writer bounds identical unless the bound
itself is the variable under test. If instrumentation or a new sample
classification is introduced, validate the harness with same-code A/A runs
before using it to support an implementation claim.

## What not to infer from tail outliers

A rare completion outlier behind a deliberately deep FIFO queue does not, by
itself, prove that the writer needs a priority scheduler, a fixed byte quantum,
extra `sleep(0)` calls or transport-level chunking. Those mechanisms change
other operating regimes and can trade one tail for more CPU, more scheduling
work or lower capacity.

First establish where the delayed frame was when the latency accumulated:
application admission, writer FIFO residence, transport buffering, broker
processing, acknowledgement handling or task scheduling. Prefer the existing
backpressure knobs when they already bound the responsible state.

See [Operations](operations.md) for the complete resource-bound and runtime
snapshot model, and [Benchmarking contract](benchmarking.md) for paired-run
validity, A/A controls and release evidence requirements.
