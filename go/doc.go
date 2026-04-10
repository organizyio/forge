// Package forge provides Organizyio Forge’s local worker orchestration runtime.
//
// Ownership boundaries:
//   - Forge owns framing, Conn RPC, WorkerProcess supervision, optional Pool
//     (start/stop only), ExtractEmbedded, ChannelEventBus, and the Rust
//     forge_sdk protocol — see forge/docs/forge-implementation-spec.md.
//   - Product packages (for example Archivist) own domain payloads, persistence,
//     scheduling, and product-level command wiring.
package forge
