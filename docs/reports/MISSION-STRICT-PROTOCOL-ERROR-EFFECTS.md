# Mission — strict `PROTOCOL_ERROR` effects

Base: `main@feb3dd307f71d54a083e73c72e2ec436b7b3f44d`

## Goal

Align Internal `PROTOCOL_ERROR` payloads with the exceptions the protocol engine can actually emit.

## Contract

- preserve valid `MalformedPacketError` / `ProtocolError` instances exactly;
- reject arbitrary payloads explicitly with `TypeError`;
- remove permissive `ProtocolError(str(data))` coercion;
- keep public exception, reconnect and reason-code behavior unchanged.

## Validation

Exact-head CI and soak must pass after reconstruction on the current main.
