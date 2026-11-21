# ADR 0047 — Precedence between decision records

Status: **Accepted** (21 November 2026, by the repository owner).

## Context

Record 0005 orders the specification chapters against each other and
against the records. Nothing ordered the records among themselves, and
several changed a rule of an earlier record without naming it: 0028
replaced the identifier rule of 0025; 0041 lifted "QoS 2 requests grant 1"
of 0026 and "outgoing deliveries remain QoS 0 or 1" of 0040; the business
threshold of the receipt window went from 768 (0027) to 640 (0029) to 512
(0030); 0030 lifted the limit of 0029 on finite expiry intervals; 0037
refused the schemas that 0035 kept serving. Accepting such records
together would have accepted texts that contradict each other.

## Decision

**Between two records, the later one prevails on the point it states.**
The number of a record is its order. A record prevails only where its text
decides the point; everything else in the earlier record stands.

**A record that changes a rule of an earlier record names that record.**
It says which rule it replaces, in its status paragraph or its decision.

**The earlier record carries a line "Amended by NNNN"** under its status,
saying which of its statements no longer holds. The line is added in the
change that introduces the later record. Nothing else in an accepted
record is rewritten: its text stays the decision that was taken.

**Status is not precedence.** A proposed record does not bind, whatever
its number: an accepted record amended by a proposed one keeps its rule
until the later record is accepted. Where the code already applies a
proposed record, the line still tells a reader what the code does.

Record 0005 keeps its subject: a chapter and a record are ordered by it,
two records by this one.

## Consequences

- The lines for the cases above were added with this record: 0025 (by
  0028), 0026 (by 0041), 0027 (by 0029 and 0030), 0029 (by 0030), 0035
  (by 0037) and 0040 (by 0041).
- The requirement register lists an accepted record with the digest of
  its file. Adding an "Amended by" line to an accepted record therefore
  changes the contract digest; the change that adds it states the new
  digest.
- The cases above were found by one reading of the forty-seven records.
  Others may exist; a reader who finds one adds the line.

## Left open

- Records 0001 to 0009 are "Accepted with amendments" and are amended by
  0011, 0012 and 0015, which name them. They carry no "Amended by" line
  yet.
