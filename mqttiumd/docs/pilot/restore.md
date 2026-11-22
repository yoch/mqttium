# Backup, actual restoration and offline onboarding

These runs used the normal extracted source 51be4079 executable, hash
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`.
SQLite remained schema 13. Source files, backups and restored directories were
private. No cleanup SQL or VACUUM was used.

The quiet case used the exact database that completed 90,000 publications and
180,000 recipient Finishes. With the broker stopped, `mqttiumd policy plan`
read the old policy and a new policy adding capteur-9. Plan succeeded on the
served 124 KiB committed image, below the tool's128 MiB image and 32 MiB logical
inventory bounds. Plan left the data unchanged. Apply created a verified backup,
including SQLite's committed source image, and atomically extended authority.
All MQTT business-table hashes matched before and after apply.

The backup was copied into a new mode 0700 directory as a mode 0600 file. Its
hash matched the tool's backup attestation before opening. A distinct broker
PID opened that copy with the old policy. Both dashboards returned Session
Present 1 without first replacing their subscriptions. They replayed the eight
expected retained topics and completed a new application publication to both
receivers. Retained bytes were checked against the captured inventory. The
original file separately reopened under the new policy and passed the same
application checks. This is an actual served-file restoration.

On the original served file, capteur-9 subsequently opened a new authenticated
session, published on its authorized status topic and reached both old
dashboards. Its certificate claiming capteur-1 received CONNACK 0x87 with
Session Present 0. Existing sessions and retained values remained present.
The public operator demo also covers refusal of policy reduction and topics
outside the new sensor's grant. Preserve both identity files and each plan.
After apply, the original file uses the new identity configuration, while the
backup uses the old configuration.

The charged case used a separate real MQTT-created database. The capture
included a transmitted QoS 1 claim with PUBACK withheld, a QoS 2 incoming exchange
awaiting PUBREL 17, an armed Will, two exact/wildcard routes, a current retained
value and its old value still owned by the first delivery. The broker was
SIGKILLed and waited with-9 while its committed WAL remained present.
`policy apply` backed up that coherent image. Equality of the full business
inventory and the backup hash was checked before restoration.

A separate restored broker resumed the original dashboard session and original
Packet Identifier 1 with DUP set. It delivered the old frozen value, the newer
value, the QoS 2 publication and the recovered Will. A subsequent retained replay
returned the current value with RETAIN 1. The original source resumed and
completed PUBREL/PUBCOMP 17. All five deliveries finished. The final capture had
zero obligations, inbound exchanges, claims and Wills, plus one retained payload.
Neither a historical claim nor an admission was recreated under a fresh identity.

Additional separate source 51be4079 CI receptions exercise committed-but-lost
results, receipt retirement, pre/post-commit SIGKILL and expected schema refusal.
The local targeted security run reopened the same file after a broker SIGKILL
and resumed an existing principal using a newly authorized certificate.

These procedures cover cooperative stopped-writer backups and process crashes.
Power interruption, torn storage writes and general format migration are outside
the received evidence. Schema 12 and 13 remain distinct. Historical schemas are
refused without reset. The operator must use one canonical regular pathname and
separate event directories, preserving the writer-lease convention.
