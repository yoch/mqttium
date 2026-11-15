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
  the archive of the checkout, named by a path relative to the campaign
  directory (`source.tar` today); its members are the files of the
  checkout as `./<path>`. `build` is the text of the build command
  (`rebuild::build_command()` of lot L4): it names placeholders (`<dir>`)
  and describes the build, it is not run as it stands; the one execution
  is `tools/check_campaign_binary.sh`. Both are required when the member
  is present. The member is named `build`, not `method`, because the bench
  already writes that text under that name in `campaign.json`. No other
  member is added: the revision is `checkout.revision`.
- The size of the source is range-checked as every 64-bit member.
- `mqttiumd_oracle produce` counts the link as established when:
  - the member is present, the archive is in the campaign, and its digest
    and size are those of the member;
  - the binary was built with its paths remapped:
    `artifact.built_with.rustflags` starts with `--remap-path-prefix=` and
    ends with `=/mqttiumd-source`;
  - read member by member as a stream, never extracted, with at most
    100 000 members and each member hashed by chunks, the archive holds
    each contract file and each oracle file the manifest lists, at its
    path, with the same digest;
  - for each path of `checkout.modified_paths` that the campaign keeps as
    an untracked file (`modified/untracked/<path>`), the archive holds the
    same content at that path.
  Without the member, the result stays provisional with the reason that
  nothing ties the binary to the recorded content; each other failure has
  its own reason.

## What a qualifying result then establishes, and what it does not

- Established, by the oracle, from the campaign alone: the result is the
  one its observations determine; the scenario, fixture, contracts and
  trace are the files it cites; the oracle that judged is the one the
  manifest identifies; the modified content of the tree is kept; and the
  campaign holds an archive with the digest the manifest names as the
  source of its binary, built with its paths remapped, which holds the
  contract and oracle files of the run and the untracked files the
  campaign keeps.
- Declared by the bench, and checkable outside the oracle: that the
  binary which ran is the one that archive builds.
  `tools/check_campaign_binary.sh` rebuilds it with the locked
  dependencies and compares digests; the oracle does not build anything,
  needs no toolchain, and does not run that check.
- Not established by the oracle:
  - that the archive is the recorded revision with its modifications:
    the revision is not checkable without git, and a modified tracked
    file is kept as a patch, which the oracle does not apply, so only the
    untracked files kept are compared with the archive;
  - that the binary which ran is the one the archive builds (the bench's
    declaration, checked by `tools/check_campaign_binary.sh`);
  - that a build of the same archive on another machine, or with another
    toolchain, gives the same binary.

## Done

The bounded check of the archive that the first version of this record
left open is part of the decision above, both parts: the contract and
oracle files, and the untracked files kept. The check of tracked files is
limited as said: the campaign keeps their patch, not the files.

## Recomputation (R1 of the audit of `a4aea95f`)

`recompute` checks the conditions above again, from the scenario and the
manifest the result cites, and refuses a qualifying result when one no
longer holds (`result-not-qualifying`, naming the condition). Until then
a result whose source was removed or changed after it was produced still
stood. `mqttiumd-lab validate` checks the result as a document and does
not check these conditions: it does not say that a result is evidence.

## What the recorded flags leave out (amendment, 15 November 2026)

`artifact.built_with.rustflags` records the flags the binary was built
with, except one thing: the directory a `--remap-path-prefix` maps from is
recorded as `<source>`, so the member reads
`--remap-path-prefix=<source>=/mqttiumd-source`. The build script of the
lab copies the flags into the binary; with the real directory in them, the
path of the unpacked source entered the binary in spite of the remapping,
and two builds of one archive in two directories were two binaries (found
by lot L4, shown by a failing check). What the prefix is mapped to is
recorded as it stands, and it is what the condition above reads.

`tools/check_campaign_binary.sh` therefore builds in a directory of its
own, whose name the run never saw: a check that passes also shows that the
binary does not depend on where its source was unpacked on that machine.
It still shows nothing about another machine or another toolchain.
