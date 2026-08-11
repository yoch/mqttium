# Vendored MQTT conformance statements

Machine-readable indexes of every normative statement in the two MQTT
specifications MQTTium implements, so a conformance claim can cite an exact
label and quote instead of a recollection.

| File | Statements |
| --- | --- |
| [`mqtt-v3.1.1-statements.json`](mqtt-v3.1.1-statements.json) | 139 |
| [`mqtt-v5.0-statements.json`](mqtt-v5.0-statements.json) | 252 |

`docs/CONFORMANCE.md` is the audit that maps these onto the implementation.
This directory is only the source material.

## Provenance

| | MQTT 3.1.1 | MQTT 5.0 |
| --- | --- | --- |
| Title | MQTT Version 3.1.1 Plus Errata 01, OASIS Standard Incorporating Approved Errata 01 | MQTT Version 5.0, OASIS Standard |
| Published | 2015-12-10 | 2019-03-07 |
| Source | [`mqtt-v3.1.1-os.html`](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) | [`mqtt-v5.0-os.html`](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html) |
| Retrieved | 2026-08-11 | 2026-08-11 |
| SHA-256 | `547c9b35…4c15d` | `fe4fd387…1d8965` |

The full SHA-256 of the retrieved bytes is recorded in each JSON file, so a
future run can prove whether OASIS republished the document underneath us.

## How the extraction works

The OASIS HTML is a Word export with a consistent convention: the text of a
normative statement is highlighted (`background:yellow`) and immediately
followed by its label in red (`[MQTT-x.y.z-n]`). `tools/extract_spec_statements.py`
reads that structure directly, so every quoted string is verbatim — nothing is
summarised, retyped, or passed through a language model. Three details are
load-bearing and were each found by a wrong result:

- **The source is `cp1252`, and declares no charset.** Decoding it as UTF-8
  replaces the en-dashes and middle dots, corrupting the very text the index
  exists to quote exactly.
- **A statement can be split across several highlighted runs** when an inline
  cross-reference link interrupts it. Adjacent runs are merged, so
  `MQTT-3.14.1-1` reads as one sentence rather than a trailing space.
- **In the appendix conformance tables the label *precedes* its text**
  (`<tr><td>[MQTT-x-n]</td><td>statement</td></tr>`). Searching backwards there
  — the direction the highlighted body form needs — silently attaches the
  *previous* row's statement, which is how `MQTT-3.8.4-3` first came out
  carrying `MQTT-3.8.4-2`'s text.

`origin` records where each statement was read: `body` for the highlighted
prose, `appendix` for the handful the body does not highlight. Where the two
renderings differ in wording — a highlighted list lead-in, an expanded pronoun —
both are kept, the appendix one under `appendix_text`. That is a property of the
source document, not a parsing artefact.

## Regenerating

```bash
python tools/extract_spec_statements.py           # re-download and rewrite
python tools/extract_spec_statements.py --check   # fail if the index has drifted
```

The indexes are generated. Fix `tools/extract_spec_statements.py` and re-run
rather than editing the JSON, and re-run the audit in `docs/CONFORMANCE.md`
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

The corresponding notice for MQTT 3.1.1 is `Copyright © OASIS Open 2014. All
Rights Reserved.` under identical terms.
