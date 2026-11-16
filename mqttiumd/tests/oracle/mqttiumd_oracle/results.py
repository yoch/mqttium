"""Producing a result, qualifying only with its evidence, and recomputing one that is offered.

A qualifying result is evidence. It is produced here, from the observations
of a run, and one that is offered is found again from the files it cites: a
result whose statuses or injection counts differ from the recomputation is
refused. The verdict of a result is never taken from the result.
"""

from __future__ import annotations

import hashlib
import pathlib
import tarfile


from . import judge
from . import recipe as recipes
from .trace import lines_of
from .semantic import reached as reached_by
from .semantic import verdict_of
from .strict import Diagnostic, Refused, parse

EXIT_CODES = {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2, "OUT_OF_SCOPE": 3}
MAX_TEXT = 1024
MAX_REASONS = 256
MAX_FAILURES = 64


def _cite(contracts, path):
    digest, size = contracts.hash_evidence(path)
    return {"path": path, "sha256": digest, "bytes": str(size)}


def _text(value: str) -> str:
    return value if len(value) <= MAX_TEXT else value[: MAX_TEXT - 1] + "…"


def _observed(run):
    """Assertion statuses and injection counts, as the observations determine them."""
    statuses = judge.judge(run)
    hits = judge.fault_hits(run)
    faults = [
        {
            "id": fault["id"],
            **{
                key: fault[key]
                for key in ("expected_hits", "expected_hits_at_least")
                if key in fault
            },
            "hits": str(hits.get(fault["id"], 0)),
        }
        for fault in run.scenario["faults"]
    ]
    return statuses, faults


def failures_of(run):
    """Bounded typed context, retained independently of the business verdict.

    The count describes every failure in this trace; the list keeps only
    its first contexts. Codes and phases come from closed schema unions,
    never from parsing diagnostic text.
    """
    total, first = 0, []
    for record in run.trace.records:
        if record.kind not in {"DurableReadFailed", "ProcessControlFailed"}:
            continue
        total += 1
        if len(first) < MAX_FAILURES:
            first.append({"kind": record.kind, "record_sequence": str(record.sequence),
                          **record.payload})
    return {"failures": first, "failure_count": str(total)} if total else {}


def produce(contracts, scenario_path, trace_path, manifest_path, scope):
    """A qualifying result for the run a trace records."""
    run = judge.Run.load(contracts, scenario_path, trace_path)
    statuses, faults = _observed(run)
    reached = all(reached_by(fault, fault["hits"]) for fault in faults)
    verdict = verdict_of([status.status for status in statuses.values()], reached)
    reasons = [
        _text(f"{name}: {reason}")
        for name, status in statuses.items()
        if status.status != "PASS"
        for reason in status.reasons
    ]
    return {
        "format": "mqttiumd.run-result",
        "schema_version": 1,
        "qualification": "qualifying",
        "run_id": run.trace.run_id or "unreadable-trace",
        "verdict": verdict,
        "exit_code": EXIT_CODES[verdict],
        "scope": scope,
        "reasons": reasons[:MAX_REASONS],
        "assertions": [
            {"name": name, "status": status.status} for name, status in statuses.items()
        ],
        "faults": faults,
        **failures_of(run),
        "scenario": _cite(contracts, scenario_path),
        "manifest": _cite(contracts, manifest_path),
        "trace": _cite(contracts, trace_path),
        "contracts_digest": contracts.digest,
        "artifacts": [],
    }


# Without artifact.built_from (record 0017), nothing ties the binary to
# the recorded content.
UNTIED_BINARY = (
    "nothing establishes that the binary was built from the recorded content "
    "(the manifest has no artifact.built_from)"
)


def unmet(contracts, scenario_path, manifest_path, trace_path=None):
    """The conditions of a qualifying run that the given files do not establish."""
    try:
        data = contracts.read(manifest_path)
    except Refused as refusal:
        return [f"the manifest cannot be read: {refusal.diagnostic}"]
    problems = contracts.check_bytes(data)
    if problems:
        return [f"the manifest is not a valid document: {problem}" for problem in problems]
    manifest = parse(data)
    if manifest["format"] != "mqttiumd.run-manifest":
        return ["the manifest is not a run manifest"]
    out = []
    if manifest["contracts_digest"] != contracts.digest:
        out.append("the manifest names other contracts than the ones in use")
    out += _cited(contracts, manifest.get("scenario"), scenario_path, "scenario")
    try:
        fixture = parse(contracts.read(scenario_path)).get("initial_fixture")
    except Refused as refusal:
        return [*out, f"the scenario cannot be read: {refusal.diagnostic}"]
    if fixture is not None:
        out += _cited(contracts, manifest.get("fixture"), fixture, "fixture")
    for member, words in (
        ("backend", "backend"),
        ("failure_model", "failure model"),
        ("scheduler", "scheduler"),
    ):
        if member not in manifest:
            out.append(f"the manifest records no {words}")
    if not manifest.get("commands"):
        out.append("the manifest records no command")
    for member in ("oracle", "generators"):
        if member not in manifest:
            out.append(f"the manifest does not identify the {member}")
    if "oracle" in manifest:
        out += _oracle_unmet(manifest["oracle"]["files"])
    if manifest["checkout"]["tree_state"] != "clean":
        out += _kept_unmet(contracts, manifest["checkout"]["modified_paths"])
    if trace_path is not None:
        out += _broker_unmet(contracts, manifest, trace_path)
        out += _scope_unmet(contracts, scenario_path, trace_path)
    out += _source_unmet(contracts, manifest)
    return out


FIXTURE_PUBSUB_SCOPE = (
    "the fixture-based qualifier does not cover outgoing claims or subscription mutations"
)
FIXTURE_QOS0_SCOPE = (
    "the fixture-based qualifier does not cover volatile or consumed QoS 0 delivery"
)
UNCORROBORATED_QUEUED = (
    "queued recovery delivery metadata is not corroborated by a durable snapshot"
)


def _scope_unmet(contracts, scenario_path, trace_path):
    """Descriptive support for a new format does not qualify its semantics.

    The fixture-based assertions cover incoming QoS 2 with queued obligations.
    They do not cover the product's subscription/claim/finish state machines.
    Queued recovery metadata remains useful in that original profile, provided
    an independently cited snapshot corroborates its durable identities.
    """
    try:
        run = judge.Run.load(contracts, scenario_path, trace_path)
    except (OSError, Refused):
        # The ordinary readers and assertions retain their own missing-proof
        # diagnostics. This condition adds no verdict for unreadable evidence.
        return []
    unsupported, non_qos2, uncorroborated, qos0_delivery = False, False, False, False
    for _, snapshot in run.snapshots.values():
        unsupported |= bool(snapshot["outbound"])
        for operation in snapshot["operations"]:
            recipe = bytes.fromhex(operation["recipe_hex"])
            if recipe[:6] == b"MQDR\x00\x01" and len(recipe) >= 7:
                unsupported |= recipe[6] in (3, 4, 5, 7)
                qos0_delivery |= recipe[6] == 6
            try:
                decoded = recipes.decode(recipe)
            except recipes.RecipeError:
                pass
            else:
                non_qos2 |= isinstance(decoded, recipes.Admit) and decoded.qos != 2
            facts = operation["decision"].get("facts", {})
            unsupported |= facts.get("kind") in {
                "SubscriptionsChanged", "DeliveryClaimed", "DeliveryFinished"
            }
            qos0_delivery |= facts.get("kind") == "DeliveryConsumed"
    for record in run.trace.records:
        payload = record.payload
        unsupported |= payload.get("command_kind") in {
            "ChangeSubscriptions", "UpdateSubscriptions", "ClaimDelivery", "FinishDelivery"
        }
        qos0_delivery |= payload.get("command_kind") == "ConsumeDelivery"
        non_qos2 |= (record.kind == "ClientBytesReceived"
                     and payload.get("packet_kind") == "PUBLISH"
                     and payload.get("qos", 2) != 2)
        unsupported |= payload.get("packet_kind") in {
            "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK"
        }
        unsupported |= record.kind == "FrameEligible" and payload.get("packet_kind") == "PUBLISH"
        if record.kind not in {"RecoveryPageRead", "RecoveryPageDelivered"}:
            continue
        for fact in payload["facts"]:
            if fact["kind"] != "OutboundDelivery":
                continue
            qos0_delivery |= fact.get("qos", 1) == 0
            if fact["claim"] is not None:
                unsupported = True
                continue
            supported = any(
                snapshot["meta"]["last_opened_epoch"] == payload["run_epoch"]
                and run.trace.before(record, inspection) is True
                and any(
                    obligation["publication_id"] == fact["delivery_id"]["publication_id"]
                    and obligation["index"] == fact["delivery_id"]["index"]
                    and obligation["session"] == fact["target_session"]
                    and obligation["target_revision"] == fact["target_revision"]
                    for obligation in snapshot["obligations"]
                )
                for inspection, snapshot in run.snapshots.values()
            )
            uncorroborated |= not supported
    return ([FIXTURE_PUBSUB_SCOPE] if unsupported else []) + (
        ["the fixture-based qualifier covers incoming QoS 2 only"] if non_qos2 else []
    ) + ([UNCORROBORATED_QUEUED] if uncorroborated else []) + (
        [FIXTURE_QOS0_SCOPE] if qos0_delivery else []
    )


def _broker_unmet(contracts, manifest, trace_path):
    """Whether the manifest and the trace header say the same of where the broker ran.

    Record 0019: the judge reads the claim of the header; the manifest
    states it for the campaign. Absent, both mean in-process.
    """
    claimed = manifest.get("broker", "in-process")
    try:
        with (contracts.root / trace_path).open("rb") as stream:
            first = next(lines_of(stream), b"")
        header = parse(first)
        stated = header["payload"].get("broker", "in-process")
    except (OSError, Refused, KeyError, TypeError):
        return ["the header of the trace cannot be read"]
    if claimed != stated:
        return [
            f"the manifest says the broker ran as a {claimed} and the trace header says {stated}"
        ]
    return []


def _source_unmet(contracts, manifest):
    """Whether the campaign holds the source its binary is declared built from.

    The oracle builds nothing. It checks that the archive the bench names
    is in the campaign, with that digest and that size, that the binary
    was built with its paths remapped as run --rebuild builds, and, reading
    the archive member by member, that it holds the contract and oracle
    files the manifest lists and the untracked files the campaign keeps.
    That the binary is the one the archive builds, and that the archive is
    the recorded revision, is the bench's declaration, checked outside the
    oracle by ``tools/check_campaign_binary.sh``.
    """
    built_from = manifest["artifact"].get("built_from")
    if built_from is None:
        return [UNTIED_BINARY]
    source = built_from["source"]
    try:
        sha256, size = contracts.hash_evidence(source["path"])
    except Refused:
        return [f"the source the manifest names, {source['path']}, is not in the campaign"]
    if sha256 != source["sha256"] or str(size) != source["bytes"]:
        return [
            f"{source['path']} of the campaign is not the source the manifest names: "
            "its digest or its size differs"
        ]
    out = []
    flags = manifest["artifact"]["built_with"]["rustflags"]
    if not (flags.startswith(REMAP_PREFIX) and flags.endswith(REMAP_TARGET)):
        out.append(
            "a binary rebuilt from its source is built with "
            f"{REMAP_PREFIX}<dir>{REMAP_TARGET}, and these rustflags are not"
        )
    listed = {entry["path"]: entry["sha256"] for entry in manifest["contracts"]}
    listed.update(
        {entry["path"]: entry["sha256"] for entry in manifest.get("oracle", {}).get("files", [])}
    )
    kept = {}
    for path in manifest["checkout"]["modified_paths"]:
        try:
            kept[path], _ = contracts.hash_evidence(f"modified/untracked/{path}")
        except Refused:
            continue  # a tracked file, kept as a patch: not comparable without applying it
    try:
        held = _archive_digests(contracts.root / source["path"], listed.keys() | kept.keys())
    except _Unreadable as unreadable:
        return [*out, f"{source['path']} {unreadable}"]
    for path, digest in sorted(listed.items()):
        if held.get(path) != digest:
            out.append(f"{source['path']} does not hold {path} as the manifest lists it")
    for path, digest in sorted(kept.items()):
        if held.get(path) != digest:
            out.append(f"{source['path']} does not hold the untracked {path} the campaign keeps")
    return out


# How run --rebuild builds: the directory of the extracted source is
# remapped, so that the binary does not depend on where it was built.
REMAP_PREFIX = "--remap-path-prefix="
REMAP_TARGET = "=/mqttiumd-source"
# The members an archive of the source may have; the checkout has about a
# thousand.
MAX_SOURCE_MEMBERS = 100_000
CHUNK = 1 << 16


class _Unreadable(Exception):
    pass


def _archive_digests(path, wanted):
    """The SHA-256 of the regular members of the archive at ``path`` named in ``wanted``.

    The archive is read as a stream, member by member, never extracted;
    a member is hashed by chunks, and the number of members is bounded.
    """
    held = {}
    count = 0
    try:
        with tarfile.open(path, mode="r|") as archive:
            for member in archive:
                count += 1
                if count > MAX_SOURCE_MEMBERS:
                    raise _Unreadable(f"has more than {MAX_SOURCE_MEMBERS} members")
                name = member.name[2:] if member.name.startswith("./") else member.name
                if not member.isreg() or name not in wanted:
                    continue
                digest = hashlib.sha256()
                stream = archive.extractfile(member)
                while chunk := stream.read(CHUNK):
                    digest.update(chunk)
                held[name] = digest.hexdigest()
    except (tarfile.TarError, OSError, EOFError) as error:
        raise _Unreadable("is not an archive the oracle can read") from error
    return held


# The checkout directory the bench names files from (``mqttiumd``), and the
# package of the oracle that runs now, below it.
CHECKOUT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE = pathlib.Path(__file__).resolve().parent


def _oracle_unmet(listed):
    """Whether the files the manifest lists are those of the oracle that runs.

    The oracle that judges is this one: its files are read from where it
    runs, not from the campaign, and each module of its package must be
    listed with its digest.
    """
    out = []
    named = {entry["path"] for entry in listed}
    for path in sorted(PACKAGE.glob("*.py")):
        relative = path.relative_to(CHECKOUT).as_posix()
        if relative not in named:
            out.append(f"the manifest does not list the oracle file {relative}")
    for entry in listed:
        path = CHECKOUT / entry["path"]
        try:
            inside = path.resolve(strict=True).is_relative_to(CHECKOUT)
            sha256 = hashlib.sha256(path.read_bytes()).hexdigest() if inside else None
        except OSError:
            sha256 = None
        if sha256 != entry["sha256"]:
            out.append(
                f"the oracle that judged is not the one the manifest identifies: {entry['path']}"
            )
    return out


# A header of a diff is short; any longer line is the content of a diff.
MAX_HEADER_BYTES = 8192


def _headers(stream):
    """The beginnings of the lines of ``stream``: a line longer than the bound is skipped whole."""
    while line := stream.readline(MAX_HEADER_BYTES + 1):
        if len(line) > MAX_HEADER_BYTES and not line.endswith(b"\n"):
            while (rest := stream.readline(MAX_HEADER_BYTES)) and not rest.endswith(b"\n"):
                pass
            continue
        yield line


def _kept_unmet(contracts, modified):
    """The modified paths the campaign does not keep, as the bench keeps them.

    An untracked path is kept as ``modified/untracked/<path>``; a tracked
    one, by its diff in ``modified/tracked.patch``.
    """
    diffed = set()
    patch = contracts.root / "modified/tracked.patch"
    if patch.is_file() and patch.resolve().is_relative_to(contracts.root):
        # A patch has no bound, a binary one has long lines: it is read by
        # bounded lines, and only the headers of its diffs are kept.
        with patch.open("rb") as stream:
            for line in _headers(stream):
                if line.startswith(b"diff --git a/"):
                    head = line.rstrip(b"\n")[len(b"diff --git a/") :]
                    diffed.add(head.split(b" b/", 1)[0].decode("utf-8", "replace"))
    out = []
    for path in modified:
        if path in diffed:
            continue
        try:
            contracts.hash_evidence(f"modified/untracked/{path}")
        except Refused:
            out.append(f"the modified path {path} is not kept in the run directory")
    return out


def _cited(contracts, cited, path, words):
    if cited is None:
        return [f"the manifest names no {words}"]
    try:
        sha256, _ = contracts.hash_evidence(path)
    except Refused as refusal:
        return [f"the {words} cannot be read: {refusal.diagnostic}"]
    if cited["sha256"] != sha256:
        return [f"the manifest names another {words} than {path}"]
    return []


def provisional(result, conditions):
    """``result`` as a development document: no evidence, so never a PASS."""
    out = dict(result, qualification="provisional")
    verdict = "INCONCLUSIVE" if result["verdict"] == "PASS" else result["verdict"]
    out.update(verdict=verdict, exit_code=EXIT_CODES[verdict])
    reasons = [_text(f"not qualifying: {condition}") for condition in conditions]
    out["reasons"] = (reasons + result["reasons"])[:MAX_REASONS]
    return out


def recompute(contracts, result_path):
    """The reasons a result is refused as evidence; empty when it stands.

    The assertions, the faults and the trace are judged again, and the
    conditions of a qualifying run (``unmet``) are checked again: a result
    stands only while every one of them holds.
    """
    try:
        data = contracts.read(result_path)
    except Refused as refusal:
        return [refusal.diagnostic]
    problems = contracts.check_bytes(data)
    if problems:
        return problems
    result = parse(data)
    if result["format"] != "mqttiumd.run-result":
        return [Diagnostic("reference-invalid", "/format", "not a run result")]
    if result["qualification"] != "qualifying":
        return [
            Diagnostic(
                "result-not-qualifying",
                "/qualification",
                "a provisional result names no evidence to recompute it from",
            )
        ]
    # The document was just checked against the digests it cites: the files
    # read now are the files it names, unless they changed in between, which
    # the digests of the run below would not hide either.
    try:
        run = judge.Run.load(contracts, result["scenario"]["path"], result["trace"]["path"])
    except Refused as refusal:
        return [refusal.diagnostic]
    try:
        sha256, _ = contracts.hash_evidence(result["trace"]["path"])
    except Refused as refusal:
        return [refusal.diagnostic]
    if sha256 != result["trace"]["sha256"]:
        return [Diagnostic("evidence-mismatch", "/trace", "the trace changed while it was read")]
    out = []
    if run.trace.run_id is not None and result["run_id"] != run.trace.run_id:
        out.append(
            Diagnostic(
                "evidence-mismatch",
                "/run_id",
                f"the cited trace is the run {run.trace.run_id}",
            )
        )
    statuses, faults = _observed(run)
    typed = {key: result[key] for key in ("failures", "failure_count") if key in result}
    if typed != failures_of(run):
        out.append(Diagnostic(
            "result-not-recomputed", "/failures",
            "the result's typed failure context or total differs from the cited trace",
        ))
    for index, entry in enumerate(result["assertions"]):
        found = statuses.get(entry["name"])
        if found is None or found.status != entry["status"]:
            out.append(
                Diagnostic(
                    "result-not-recomputed",
                    f"/assertions/{index}/status",
                    f"{entry['name']} is {entry['status']} in the result and "
                    f"{found.status if found else 'absent'} in the observations: "
                    f"{'; '.join(found.reasons) if found else ''}"[:MAX_TEXT],
                )
            )
    counted = {fault["id"]: fault["hits"] for fault in faults}
    for index, entry in enumerate(result["faults"]):
        if counted.get(entry["id"]) != entry["hits"]:
            out.append(
                Diagnostic(
                    "result-not-recomputed",
                    f"/faults/{index}/hits",
                    f"{entry['id']} was reached {counted.get(entry['id'])} times in the trace",
                )
            )
    # R1: a qualifying result is evidence only while the conditions it was
    # produced under hold. They are checked again from the scenario and the
    # manifest it cites, as produce checks them; a broken one refuses it.
    cited = (result["scenario"]["path"], result["manifest"]["path"], result["trace"]["path"])
    for condition in unmet(contracts, *cited):
        out.append(
            Diagnostic(
                "result-not-qualifying",
                "/manifest",
                _text(f"a condition of a qualifying run no longer holds: {condition}"),
            )
        )
    return out
