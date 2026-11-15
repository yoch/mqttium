# 0017 — The binary of a run tied to the source it was rebuilt from

Status: **Proposed** (15 November 2026, lot L3, second wave). Written by
lot L3 at the request of the integration owner, to be recorded by them. It
completes DEM-003 and V3-002 and closes, for runs made with `--rebuild`,
the open condition "the binary of a campaign is built from the recorded
content inside the campaign, or its correspondence to that content is
otherwise established" (lot L4). It becomes a requirement only when the
owner of the repository accepts it.

## Context

A run manifest keeps apart what is certain about the binary that ran
(`artifact`: its digest and its build parameters) from what was observed
around it (`checkout`). It does not claim that the binary was built from
that checkout. A qualifying result needs that claim: without it, a PASS
could be the verdict of a binary built from anything. `run --rebuild`
builds the binary from an archive of the recorded content, keeps the
archive in the campaign as `source.tar`, and states the link in
`campaign.json` (`built_from`), which no contract reads.

## Decision

- `run-manifest` gains `artifact.built_from`, optional, written only by a
  run whose binary the bench rebuilt from a source it keeps:
  `{"source": {"path", "sha256", "bytes"}, "build": <text>}`. `source` is
  a file of the campaign, named by a relative path; `build` is the command
  that built it. Both are required when the member is present. The member
  is named `build`, not `method`, because the bench already writes that
  text under that name in `campaign.json`.
- The size of the source is range-checked as every 64-bit member.
- `mqttiumd_oracle produce` counts the link as established when the member
  is present, the archive is in the campaign, and its digest and size are
  those of the member. Without the member, the result stays provisional
  with the reason that nothing ties the binary to the recorded content.
  When the member names an archive that is missing or differs, the reason
  says so.

## What a qualifying result then establishes, and what it does not

- Established, by the oracle, from the campaign alone: the result is the
  one its observations determine; the scenario, fixture, contracts and
  trace are the files it cites; the oracle that judged is the one the
  manifest identifies; the modified content of the tree is kept; and the
  campaign holds an archive with the digest the manifest names as the
  source of its binary.
- Declared by the bench, and checkable outside the oracle: that the
  binary which ran is the one that archive builds.
  `tools/check_campaign_binary.sh` rebuilds it with the locked
  dependencies and compares digests; the oracle does not build anything,
  needs no toolchain, and does not run that check.
- Not established: that a build of the same archive on another machine,
  or with another toolchain, gives the same binary.

## Open

A bounded check of the archive that the oracle could make without a
toolchain is proposed, not decided: each contract file and each oracle
file the manifest lists is found in the archive, at its path, with the
same digest, read member by member with a bound on the number of members
and on the size of each. It would tie the archive to the contracts and
the oracle of the run, not to the binary.
