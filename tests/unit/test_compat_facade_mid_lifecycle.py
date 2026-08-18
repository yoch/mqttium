from __future__ import annotations

from mqttium.api.models import PublishReceipt
from mqttium.compat.paho import Client, _MAX_FACADE_MID
from mqttium.enums import ConnectionState, QoS


def test_facade_mid_wrap_refuses_live_collision_then_reuses_after_settle() -> None:
    client = Client()
    client._last_facade_mid = _MAX_FACADE_MID

    mid, reserved = client._reserve_next_facade_mid()
    assert (mid, reserved) == (1, True)

    receipt = PublishReceipt(mid=41, qos=QoS.AT_LEAST_ONCE)
    client._register_facade_mid(receipt, mid)

    client._last_facade_mid = _MAX_FACADE_MID
    collided_mid, reserved = client._reserve_next_facade_mid()
    assert (collided_mid, reserved) == (1, False)

    receipt._settle()

    client._last_facade_mid = _MAX_FACADE_MID
    reused_mid, reserved = client._reserve_next_facade_mid()
    assert (reused_mid, reserved) == (1, True)


def test_facade_mid_is_retired_without_user_callback_or_waiter() -> None:
    client = Client()
    facade_mid, reserved = client._reserve_next_facade_mid()
    assert reserved

    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    client._register_facade_mid(receipt, facade_mid)
    assert client._facade_receipts[id(receipt)] == (receipt, 7, facade_mid)
    assert 7 not in client._facade_mid_map
    assert facade_mid in client._active_facade_mids

    receipt._settle()

    assert facade_mid not in client._active_facade_mids
    assert 7 not in client._facade_mid_map
    assert not client._facade_receipts


def test_callback_installed_after_publish_keeps_mid_active_through_callback() -> None:
    client = Client()
    client._async._engine.state = ConnectionState.CONNECTED
    facade_mid, reserved = client._reserve_next_facade_mid()
    assert reserved

    receipt = PublishReceipt(mid=23, qos=QoS.AT_LEAST_ONCE)
    client._register_facade_mid(receipt, facade_mid)

    seen: list[tuple[int | None, bool]] = []

    def on_publish(
        _client: Client,
        _userdata: object,
        mid: int | None,
        _rc: int,
        _props: object,
    ) -> None:
        seen.append((mid, facade_mid in client._active_facade_mids))

    client._install_publish_dispatch(on_publish)

    receipt._settle()
    assert facade_mid in client._active_facade_mids
    client._dispatch_publish(23, None)

    assert seen == [(facade_mid, True)]
    assert 23 not in client._facade_mid_map
    assert facade_mid not in client._active_facade_mids
    assert not client._facade_receipts


def test_removing_callback_does_not_drop_already_queued_correlation() -> None:
    client = Client()
    client._async._engine.state = ConnectionState.CONNECTED
    client._install_publish_dispatch(lambda *_args: None)
    facade_mid, reserved = client._reserve_next_facade_mid()
    assert reserved

    receipt = PublishReceipt(mid=29, qos=QoS.EXACTLY_ONCE)
    client._register_facade_mid(receipt, facade_mid)
    receipt._settle()

    # AsyncClient can already have queued the dispatcher when the user removes
    # the callback. The mapping and reservation must survive until that queued
    # dispatcher runs, even though it will find no user callback to invoke.
    assert facade_mid in client._active_facade_mids
    client._install_publish_dispatch(None)
    client._dispatch_publish(29, None)

    assert 29 not in client._facade_mid_map
    assert facade_mid not in client._active_facade_mids
    assert not client._facade_receipts


def test_callback_owned_binding_retains_receipt_identity_until_dispatch() -> None:
    client = Client()
    client._async._engine.state = ConnectionState.CONNECTED
    client._install_publish_dispatch(lambda *_args: None)
    facade_mid, reserved = client._reserve_next_facade_mid()
    assert reserved

    receipt = PublishReceipt(mid=17, qos=QoS.AT_LEAST_ONCE)
    receipt_id = id(receipt)
    client._register_facade_mid(receipt, facade_mid)
    receipt._settle()

    # The queued dispatcher is now the owner of the façade MID. Keep the receipt
    # itself alive until that dispatcher consumes the binding: retaining only
    # id(receipt) would allow CPython to reuse the address for a newer receipt.
    bound_receipt, real_mid, bound_facade_mid = client._facade_receipts[receipt_id]
    assert bound_receipt is receipt
    assert (real_mid, bound_facade_mid) == (17, facade_mid)

    client._dispatch_publish(17, None)
    assert receipt_id not in client._facade_receipts
    assert facade_mid not in client._active_facade_mids


def test_final_bulk_failure_retires_mid_even_with_publish_callback_installed() -> None:
    client = Client()
    client._async._engine.state = ConnectionState.CONNECTED
    client._install_publish_dispatch(lambda *_args: None)
    facade_mid, reserved = client._reserve_next_facade_mid()
    assert reserved

    receipt = PublishReceipt(mid=31, qos=QoS.AT_LEAST_ONCE)
    client._async._register_publish_receipt(31, receipt)
    client._register_facade_mid(receipt, facade_mid)

    failure = RuntimeError("connection lost")
    client._async._fail_pending(failure)

    assert receipt.is_done()
    assert receipt._error is failure
    assert facade_mid not in client._active_facade_mids
    assert 31 not in client._facade_mid_map
    assert not client._facade_receipts
