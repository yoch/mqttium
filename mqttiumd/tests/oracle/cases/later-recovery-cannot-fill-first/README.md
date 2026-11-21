# First recovery without a complete inventory

Synthetic counterexample supplied with the review of `edd8acbb`. It is not
a broker execution and is not a qualifying campaign. After a K2b kill of
incarnation 1, startup 2 supplies no inventory; startup 3, after another
kill, supplies a complete inventory. The later pages cannot prove what the
first restarted incarnation recovered.

`test_first_recovery.py` copies this case beside the current checkout's
contracts and fixture and invokes the ordinary oracle CLI. It asserts the
current contract identity explicitly; an old-digest or schema rejection
does not count as detection. It also constructs a structurally valid
partial inventory of startup 2 from these observations.

The descriptive snapshots include the empty outgoing-claim table of ADR 0025
and empty compact receipt-frontier table of ADR 0027.
Their citations were regenerated with that format; the missing first inventory
and the synthetic history are unchanged.
