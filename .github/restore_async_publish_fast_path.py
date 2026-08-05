from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:120]!r}")
    file.write_text(text.replace(old, new, 1))


path = "src/mqttium/api/async_client.py"
replace_once(
    path,
    """                    receipt = self._queue_publish_on_loop(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                    )
""",
    """                    # Keep the native async hot path inline. Routing these
                    # operations through the adapter boundary measured 2.36% slower.
                    handle = self._engine.queue_publish(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                    )
                    if handle.qos == QoS.AT_MOST_ONCE:
                        receipt = PublishReceipt(mid=None, qos=handle.qos, _event=None)
                    else:
                        assert handle.mid is not None
                        receipt = PublishReceipt(
                            mid=handle.mid,
                            qos=handle.qos,
                            _event=asyncio.Event(),
                        )
                        self._register_publish_receipt(handle.mid, receipt)
""",
)
replace_once(
    path,
    """                else:
                    self._finalize_loop_commands()
            if not wait_for_space:
""",
    """                else:
                    self._collect_effects_locked()
                    self._drain_effects_inline()
            if not wait_for_space:
""",
)
replace_once(
    path,
    """            mid = self._queue_subscribe_on_loop(
                topics,
                qos=qos,
                properties=properties,
            )
""",
    """            mid = self._engine.queue_subscribe(
                topics,
                qos=qos,
                properties=properties,
            )
""",
)
replace_once(
    path,
    "            mid = self._queue_unsubscribe_on_loop(topics)\n",
    "            mid = self._engine.queue_unsubscribe(topics)\n",
)

# Record why the apparently duplicated admission sequence is deliberate.
design = Path("docs/DESIGN.md")
text = design.read_text()
old = """La façade Paho ne touche plus directement `ProtocolEngine`, les registres de
receipts ou `EffectPump`. Elle passe par une petite frontière interne
loop-confined d'`AsyncClient`, afin de préserver le batching et les fast paths
sans introduire de bus de commandes générique.
"""
new = """La façade Paho ne touche plus directement `ProtocolEngine`, les registres de
receipts ou `EffectPump`. Elle passe par une petite frontière interne
loop-confined d'`AsyncClient`, afin de préserver le batching et les fast paths
sans introduire de bus de commandes générique. Le chemin natif
`await publish()` garde volontairement l'admission, la création du receipt et
le drainage inline : le faire passer par ces wrappers a mesuré 2,36 % plus lent
sur le contrôle apparié, au-delà du budget de régression.
"""
if text.count(old) != 1:
    raise SystemExit("DESIGN adapter paragraph missing or duplicated")
design.write_text(text.replace(old, new, 1))

Path(".github/restore_async_publish_fast_path.py").unlink()
Path(".github/workflows/restore-async-fast-path.yml").unlink()
