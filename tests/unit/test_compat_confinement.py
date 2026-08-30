"""Thread confinement for the synchronous compatibility façade."""

from __future__ import annotations

import threading

from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import QoS


def test_mutation_handoff_runs_on_network_thread() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    caller = threading.get_ident()
    client.loop_start()
    try:
        loop_thread = client._run_loop_mutation(threading.get_ident)
        assert client._thread is not None
        assert loop_thread == client._thread.ident
        assert loop_thread != caller
    finally:
        client.loop_stop()


def test_configuration_setters_are_safe_after_loop_start() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    callback = lambda *args: None
    client.loop_start()
    try:
        client.user_data_set({"key": "value"})
        client.username_pw_set("user", "secret")
        client.will_set("will/topic", "payload", qos=1, retain=True)
        client.message_callback_add("sensor/+", callback)

        values = client._run_loop_mutation(
            lambda: (
                client._userdata,
                client._async._engine.config.username,
                client._async._engine.config.password,
                client._async._engine.config.will,
                list(client._topic_callbacks.iter_match("sensor/1"))
                if client._topic_callbacks is not None
                else [],
            )
        )
        userdata, username, password, will, callbacks = values
        assert userdata == {"key": "value"}
        assert username == "user"
        assert password == b"secret"
        assert will is not None
        assert will.topic == "will/topic"
        assert will.payload == b"payload"
        assert will.qos is QoS.AT_LEAST_ONCE
        assert will.retain is True
        assert callbacks == [callback]

        client.message_callback_remove("sensor/+")
        assert client._run_loop_mutation(lambda: client._topic_callbacks) is None
    finally:
        client.loop_stop()


def test_mutation_from_network_thread_does_not_deadlock() -> None:
    client = Client(CallbackAPIVersion.VERSION2)
    client.loop_start()
    try:
        done = threading.Event()
        errors: list[BaseException] = []

        def run_on_loop() -> None:
            try:
                client.username_pw_set("callback-user", b"pw")
                client.message_callback_add("inside/#", lambda *args: None)
            except BaseException as exc:
                errors.append(exc)
            finally:
                done.set()

        assert client._loop is not None
        client._loop.call_soon_threadsafe(run_on_loop)
        assert done.wait(timeout=2.0)
        assert errors == []
    finally:
        client.loop_stop()
