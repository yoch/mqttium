from pathlib import Path

p = Path("src/mqttium/protocol/inbound.py")
text = p.read_text()
old = '''    def _on_qos2(
        self,
        *,
        topic: str,
        payload: bytes,
        mid: int,
        retain: bool,
        dup: bool,
        properties: Properties | None,
    ) -> None:
        engine = self._engine
        config = self.config
        store = self.store
'''
new = '''    def _on_qos2(
        self,
        *,
        topic: str,
        payload: bytes,
        mid: int,
        retain: bool,
        dup: bool,
        properties: Properties | None,
    ) -> None:
        engine = self._engine
        store = self.store
'''
count = text.count(old)
if count != 1:
    raise RuntimeError(f"expected one _on_qos2 config block, found {count}")
p.write_text(text.replace(old, new, 1))
