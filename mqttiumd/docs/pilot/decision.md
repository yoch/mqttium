# Pre-pilot decision

**NO-GO for the proposed 50-publication/s private pilot with persistent
dashboard outages.** The qualified executable is
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`, built
from `51be4079f7ee42f46500e993315eddc971f0a8d4`. The package preserves those
bytes and identifies its separate documentation revision in `PACKAGE.json`.

The healthy-load objective passed. Eight certificate-authenticated sensors and
two persistent dashboards completed 90,000 incoming QoS 1 admissions and 180,000
received, durably finished outgoing deliveries in 1,800 seconds. The PUBACK p99
was 7.29 ms, and maximum sampled RSS was 9.50 MiB. No unexplained admitted-message
loss, duplicate durable operation, authorization admission or process stop was
observed. Three confirmed P1 runtime races in reference `49bbaaa4` were corrected
with separate red-test commits and requalified on the downloaded normal build.

The operating profile fails its dashboard-outage requirement at the proposed
rate. With one dashboard offline, the unchanged 128-reference budget accepted
127 of 500 publications during a 10-second, 50/s run. It refused 373 with PUBACK
`0x97`, starting at 2.54 seconds. The online dashboard received exactly the
accepted set, the offline dashboard recovered all 127, and new admissions resumed
after drainage. The independent control connection also progressed. These are
correct quota refusals. They establish a service-capacity limitation for the
requested workload. The demonstration has no received producer back-pressure
or durable producer-spooling contract that makes those refusals compatible with
continuous collection. The 50/s target has not been reduced after measurement.

A GO with restrictions would require a verified operating contract for those
outages. That contract is outside the evidence delivered here. The code's hard
quotas have been preserved. This decision does not attribute corruption or an
MQTT safety failure to the measured capacity refusal.

| Initial criterion | Result on the reference machine |
| --- | --- |
|50 QoS 1 publications/s for 30 minutes|PASS, 90,000 positive PUBACKs and 180,000 durable recipient Finishes|
|Publication → PUBACK p99≤250 ms|PASS, 7.29 ms. Maximum 308.47 ms|
|Detected loss → durable retained Will p99≤250 ms|PASS, 11.31 ms over 100 TLS samples|
|Will commit → live receipt p99≤250 ms|PASS, 10.23 ms over 100 TLS samples|
|Broker RSS≤64 MiB|PASS in the measured workload, sampled 8.45–9.50 MiB|
|Unauthorised admissions|0 observed in the targeted security matrix|
|Unexplained loss or duplicate durable admission|0 observed for the accepted responsibilities|
|Two actual restorations|PASS, served-load backup and separate charged capture|
|Stability|Healthy run completed, all ten transports closed normally with zero Wills|
|Persistent outage at the proposed rate|NOT RECEIVED for continuous collection, 373/500 rejected|

The reference is a shared Linux x86_64 environment, kernel 6.18.57, glibc 2.41,
AMD EPYC 9V74, five affinity CPUs with a four-CPU cgroup quota and 16 GiB memory
limit. Storage is overlayfs with an unidentified underlying medium. All load
clients connected through real loopback TLS 1.3. See [load.md](load.md) for the
measured envelope, outliers and I/O cost, [restore.md](restore.md) for actual
recovery and onboarding, and [security.md](security.md) for the targeted controls.

The review is bounded to the four stated areas. Its durability subreview has
limited independent coverage, and the 1,571-file PR has no claim of complete
independent review. Full-window internal queue peaks and hardware storage
endurance remain unqualified. Clock origin recognition was unavailable, and
Message Expiry retains its explicit quarantine policy. Process SIGKILL tests
provide process-recovery evidence. The existing K1/K2b/K3 results retain their
own semantic scope.

The distributed archive is an executable evaluation package with an operator
guide and reproducible receptions. A subsequent pilot decision needs a received
capacity/outage contract and its corresponding resource evidence. The evidence supports the present NO-GO decision.
