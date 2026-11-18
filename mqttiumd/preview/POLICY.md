# Onboard a device on an existing mTLS database

Run the packaged demonstration with Python 3, OpenSSL and the Mosquitto
client tools.

```sh
sha256sum -c SHA256SUMS
./policy-demo.py ./onboarding-demo
```

The script creates temporary credentials, a persistent dashboard
subscription, an old sensor session, a retained state and two offline QoS 1
deliveries. It stops the broker, plans and applies the addition of
`capteur-2`, resumes the old sessions on the same SQLite file and drains the
deliveries. The second sensor publishes on its own topic. A foreign ClientID
and an overbroad filter receive reason 0x87. The script also rejects a grant
reduction and recovers the old backlog from a separate copy of the backup.
Keep the output directory private. Its SQLite files contain message data.

## Plan and apply

Use the same database pathname as the broker, on a local filesystem. Stop
the broker and other writers. Keep the exact previous identities configuration
and prepare the proposed configuration with actual certificate fingerprints.
`identities.example.json` and `identities.next.example.json` illustrate the
addition. Their pins are placeholders. See [SECURE.md](SECURE.md) for
certificate provisioning.

```sh
umask 077
mkdir -m 700 ./private-backups
./mqttiumd policy plan --db ./secure-data/broker.sqlite \
  --old-identities ./old-identities.json --new-identities ./new-identities.json \
  --out ./private-backups/onboard.json

./mqttiumd policy apply --db ./secure-data/broker.sqlite \
  --old-identities ./old-identities.json --new-identities ./new-identities.json \
  --plan ./private-backups/onboard.json --backup ./private-backups/before.sqlite
```

Inspect the `Planned` result, especially its additions, counts and three
fingerprints. The plan file is canonical. Keep it as written by the tool.
It describes the exact committed state at planning time. A later change to
the state, including a broker restart, can make it stale. Prepare a fresh
plan after stopping the writer again. The planner uses a read-only SQLite
snapshot and can prepare a preliminary inventory while a writer runs.

`Applied` means the backup has been verified, the authority transaction has
committed and the subsequent read has confirmed the complete new authority.
Keep the original config next to the backup. Deploy the new config as
`identities.json` in the directory used by `start-tls.sh` and restart the
broker on the same file.

The tool checks literal MQTT levels. Moving a grant from
`atelier/capteur-1/command` to `atelier/capteur-1` preserves the old subtree.
Moving it to `atelier/capteur-10` is refused. Removal of grants, principals
or ClientIDs and transfers of existing ClientIDs are refused. The plan
also refuses reassigning a pin that appears under different principals in
the two supplied files. Storage authenticates authorization fingerprints.
Certificate pins in the supplied old file remain an operator attestation.
Rotate certificates within a principal. A credential-only plan reports
`Unchanged` and the ordinary restart procedure applies the new pins.

## Resolve an interrupted apply

Keep the broker stopped and repeat the same command. `AlreadyApplied`
confirms the complete new registry and the planned business fingerprint.
If the original authority still holds and the backup path is occupied,
repeat with a fresh private backup pathname. Keep any `.pending` file
separate from usable backups. A different recorded authority or a stale
inventory requires investigation or a new verified plan. A denied plan has
`applicable: false` and cannot be applied.

Output is one JSON document. Operational causes include `wrong_schema`,
`old_policy_mismatch`, `unknown_durable_authority`,
`non_monotone_policy_change`, `invalid_durable_state`, `capacity_exceeded`,
`writer_alive`, `backup_failed` and `stale_plan`. `commit_unknown` and
`readback_failed` require a fresh inspection before reporting success.
The accepted format is secure schema 13 with the broker's existing inventory
budgets and a 128 MiB page-image limit for this offline operation.

## Restore

Stop the broker. Verify the backup against the SHA-256 returned by
`apply`. SQLite's backup API included the committed WAL pages in this
standalone file. Copy it to a **new** private directory, then start the
broker there with the old identities and matching credentials.

```sh
umask 077
mkdir -m 700 ./restored-data
cp ./private-backups/before.sqlite ./restored-data/broker.sqlite
./start-tls.sh ./restored-data ./old-tls-configuration
```

Keep the current data set and its sidecars intact. A backup is a point in
time: work accepted after it was taken belongs to the current data set.
Coordinate restoring a copy as a deliberate operator decision before
redirecting clients. Keep only one serving instance for a data set.
