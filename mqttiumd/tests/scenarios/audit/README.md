# Scenarios of the audit of the first wave

Regressions of the findings in `docs/reception/open-conditions.md`, played
by the runner of lot L4 (`tools/run_campaign.sh`).

| Scenario | Finding | What it plays | Limits |
| - | - | - | - |
| `F3-steady-lag.json` (`F3-STEADY-LAG`) | F3 | The first `Persist` commits and its result is lost; seven resolutions run, one per deadline, and each answer is held; the answer to the first is handed over after six newer resolutions ran. The exchange must commit: PUBREC follows the late answer | One late answer, with a lag of six. The lag is bounded by the result slots of the fixture (8: seven held answers and one command in transit). A steady regime where every answer arrives four resolutions late is not written: the scenario would have to know in advance at which delivery the core progresses, since no resolution follows a commit. Its control is mutation `m4` of `tests/mutants/loop/`, which the oracle leaves INCONCLUSIVE: no assertion of the catalogue states progress |
