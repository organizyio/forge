# Forge Rust workspace

## Windows: `cannot find crt2.o` / `x86_64-w64-mingw32-gcc` link errors

That means the default Rust host is **`x86_64-pc-windows-gnu`** but MinGW-w64 is missing or incomplete. Easiest fix: use the **MSVC** toolchain (install [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) with the “Desktop development with C++” workload), then either:

```powershell
rustup default stable-x86_64-pc-windows-msvc
```

or only for this tree:

```powershell
cd forge\rust
rustup override set stable-x86_64-pc-windows-msvc
```

Alternatively, install a full MinGW-w64 (e.g. via MSYS2) and ensure its `bin` is on `PATH` ahead of other `gcc` copies.

## Build

From `forge/rust`:

```bash
cargo test -p forge-worker-sdk
cargo build -p minimal-worker
```
