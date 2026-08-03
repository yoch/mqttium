from pathlib import Path

path = Path("src/mqttium/api/async_client.py")
text = path.read_text()
old = '''        # Cross-thread engine/receipt mutations for the Paho compat façade.
        # Held around queue_publish + receipt registration so off-loop publish
        # can allocate a mid without waiting for the asyncio loop tick.
        self._state_mutex = threading.RLock()
'''
new = '''        # Serializes short compat engine/effect/receipt mutations with native
        # async paths. The Paho façade enters the engine on its owning loop;
        # this mutex keeps each synchronous batch atomic until effects are taken.
        self._state_mutex = threading.RLock()
'''
if text.count(old) != 1:
    raise SystemExit(f"expected one state-mutex comment, found {text.count(old)}")
path.write_text(text.replace(old, new, 1))
