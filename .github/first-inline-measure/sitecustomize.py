"""Diagnostic-only import hook; never present in uninstrumented timing phases."""
import atexit
from collections import Counter
import functools
import importlib.abc
import importlib.machinery
import json
import os
from pathlib import Path
import sys

OUT = os.environ.get('MQTTIUM_BATCH_DIAG')
COUNTS = Counter()
ORIGINS = set()
LIMITS = set()


def install(module):
    cls = module.ApplicationDelivery
    ORIGINS.add(module.__file__)
    for name, position in [('_enqueue_message_batch', 1),
                           ('_dispatch_sync_message_pair_inline', 1),
                           ('_dispatch_sync_message_burst_inline', 1),
                           ('deliver_callback_messages_inline', 0)]:
        original = getattr(cls, name, None)
        if original is None:
            continue
        def wrap(method, label, pos):
            @functools.wraps(method)
            def call(self, *args, **kwargs):
                COUNTS[f'{label}:{len(args[pos])}'] += 1
                LIMITS.add((self.mode, self.small_message_limit, self._callback_limit))
                return method(self, *args, **kwargs)
            return call
        setattr(cls, name, wrap(original, name, position))
    original = cls.dispatch_callback_inline
    @functools.wraps(original)
    def dispatch(self, callback, *args):
        name = 'message' if args and hasattr(args[0], 'payload') else 'other'
        COUNTS['inline_dispatch:' + name] += 1
        return original(self, callback, *args)
    cls.dispatch_callback_inline = dispatch


class Loader:
    def __init__(self, original):
        self.original = original
    def create_module(self, spec):
        return self.original.create_module(spec)
    def exec_module(self, module):
        self.original.exec_module(module)
        install(module)


class Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname != 'mqttium.api._delivery':
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is not None:
            spec.loader = Loader(spec.loader)
        return spec


def save():
    if not ORIGINS:
        return
    out = Path(OUT)
    out.mkdir(parents=True, exist_ok=True)
    (out / f'{os.getpid()}.json').write_text(json.dumps({
        'diagnostic_only': True, 'pid': os.getpid(), 'argv': sys.argv,
        'origins': sorted(ORIGINS), 'limits': sorted(LIMITS, key=str),
        'counts': dict(COUNTS)}, indent=2) + '\n')


if OUT:
    sys.meta_path.insert(0, Finder())
    atexit.register(save)
