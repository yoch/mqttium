# Vendored MQTT conformance statements

Machine-readable indexes of the numbered conformance statements in the two MQTT
specifications MQTTium implements, so a conformance claim can cite an exact
version, label and quote instead of a recollection.

| File | Statements |
| --- | --- |
| [`mqtt-v3.1.1-statements.json`](mqtt-v3.1.1-statements.json) | 139 |
| [`mqtt-v5.0-statements.json`](mqtt-v5.0-statements.json) | 251 |

`docs/conformance.md` is the audit that maps these onto the implementation.
This directory is only the source material.

## Provenance

| | MQTT 3.1.1 | MQTT 5.0 |
| --- | --- | --- |
| Title | MQTT Version 3.1.1 Plus Errata 01, OASIS Standard Incorporating Approved Errata 01 | MQTT Version 5.0, OASIS Standard |
| Published | 2015-12-10 | 2019-03-07 |
| Source | [`mqtt-v3.1.1-errata01-os-complete.html`](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html) | [`mqtt-v5.0-os.html`](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html) |
| Archive | [`mqtt-v3.1.1-errata01-os.zip`](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os.zip) | [`mqtt-v5.0-os.zip`](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.zip) |
| Retrieved | 2026-08-11 | 2026-08-11 |
| Archive SHA-256 | `7c3932da…508516e` | `948793b3…a5931e` |
| HTML SHA-256 | `df463403…5651f5` | `4326d279…35c67` |

The generator downloads the official ZIP archives, not the live HTML URLs.
Those archives are byte-stable; the live pages are served through a layer that
rewrites email-obfuscation tokens and therefore changes their hash without a
specification change. Both the archive and the selected HTML member hashes are
recorded in each JSON file.

## How the extraction works

The OASIS HTML is a Word export with a useful convention: numbered normative
text is highlighted (`background:yellow`) and followed by a label
(`[MQTT-x.y.z-n]`). `tools/extract_spec_statements.py` parses that structure with
the standard-library HTML parser; nothing is summarised, retyped or passed
through a language model. The appendix conformance table is parsed independently
and used for the few statements absent from the highlighted body.

Three details are deliberately tested:

- Both HTML files declare `windows-1252` (`cp1252` in Python). Decoding as UTF-8
  corrupts punctuation.
- Highlighted text can contain nested spans and links. Element-stack tracking
  is required; matching the first closing `</span>` reduced
  `MQTT-3.1.2-11` to the fragment `(0x00)` in the first implementation.
- In appendix rows the label precedes its text. Rows are parsed as cells instead
  of searching backwards through flattened markup.

`origin` records where each statement was read: `body` for the highlighted
prose, `appendix` for the handful the body does not highlight. Where the two
renderings differ, both are kept, with the appendix rendering under
`appendix_text`.

The MQTT 5 body labels its network-connection rule `MQTT-4.2-1`; the appendix
labels the same text `MQTT-4.2.0-1`. The index preserves the latter as
`appendix_id` on the body statement rather than pretending these are two rules.
This is why the MQTT 5 index contains 251 statements, not the naïve union of 252
labels.

## Regenerating

```bash
python tools/extract_spec_statements.py           # re-download and rewrite
python tools/extract_spec_statements.py --check   # fail if the index has drifted
# or use already downloaded official archives:
python tools/extract_spec_statements.py --from-archive V311_ZIP V5_ZIP
```

The indexes are generated. Fix `tools/extract_spec_statements.py` and re-run
rather than editing the JSON, and re-run the audit in `docs/conformance.md`
whenever the statement set changes.

## Copyright

The statement texts are reproduced from the OASIS specifications named above.
Reproduced under the terms in those documents, which permit copying in whole or
in part in works that assist in their implementation, provided the following
notice is included:

> Copyright © OASIS Open 2019. All Rights Reserved.
>
> All capitalized terms in the following text have the meanings assigned to them
> in the OASIS Intellectual Property Rights Policy (the "OASIS IPR Policy"). The
> full Policy may be found at the OASIS website.
>
> This document and translations of it may be copied and furnished to others,
> and derivative works that comment on or otherwise explain it or assist in its
> implementation may be prepared, copied, published, and distributed, in whole
> or in part, without restriction of any kind, provided that the above copyright
> notice and this section are included on all such copies and derivative works.
> However, this document itself may not be modified in any way, including by
> removing the copyright notice or references to OASIS, except as needed for the
> purpose of developing any document or deliverable produced by an OASIS
> Technical Committee (in which case the rules applicable to copyrights, as set
> forth in the OASIS IPR Policy, must be followed) or as required to translate it
> into languages other than English.
>
> The limited permissions granted above are perpetual and will not be revoked by
> OASIS or its successors or assigns.
>
> This document and the information contained herein is provided on an "AS IS"
> basis and OASIS DISCLAIMS ALL WARRANTIES, EXPRESS OR IMPLIED, INCLUDING BUT NOT
> LIMITED TO ANY WARRANTY THAT THE USE OF THE INFORMATION HEREIN WILL NOT
> INFRINGE ANY OWNERSHIP RIGHTS OR ANY IMPLIED WARRANTIES OF MERCHANTABILITY OR
> FITNESS FOR A PARTICULAR PURPOSE.

The corresponding notice for MQTT 3.1.1 Plus Errata 01 is
`Copyright © OASIS Open 2015. All
Rights Reserved.` under identical terms.
