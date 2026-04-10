# Forge Protocol Compatibility

The protocol version is tracked in `protocol/VERSION`.

## Version Matrix

| Protocol | Go SDK | Rust `forge-worker-sdk` | `organizy-worker` |
|----------|--------|------------------|--------------------|
| 1.0 | 0.1.x | 0.1.x | 0.1.x |

## Release Rules

- Patch releases keep backward compatibility.
- Minor releases allow additive fields and methods.
- Major releases indicate breaking API or protocol behavior.
