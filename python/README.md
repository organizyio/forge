# forge-worker-sdk (Python)

Python peer of the Rust `forge-worker-sdk` crate. Speaks Forge framed IPC over Unix domain sockets or framed stdio.

## Install

```bash
pip install forge-worker-sdk
# or
uv add forge-worker-sdk
```

From source (contrib / tests):

```bash
cd forge/python
python3 -m pip install -e ".[dev]"
FORGE_WORKER_TEST=1 pytest
```

## Example worker

```bash
python3 examples/minimal-python-worker/main.py --socket /tmp/forge-py.sock --encoding json
```

See [docs/python-worker.md](../docs/python-worker.md).
