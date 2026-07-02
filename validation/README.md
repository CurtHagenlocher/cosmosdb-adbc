# Validation harness

End-to-end checks that load the built `adbc_cosmos` **cdylib** through a real ADBC driver
manager, exercising the C ABI / Arrow C Data Interface exactly as a Python/pyarrow (or R,
Go, DuckDB, …) consumer would — complementing the in-process Rust tests in
`crates/adbc-cosmos/tests/`.

## `roundtrip.py`

Loads `target/debug/adbc_cosmos.dll` via `adbc_driver_manager` and asserts:

- **native dialect / json** — single `document` column, `arrow.json` extension type survives
  the FFI boundary (pyarrow promotes it to its canonical `JsonType`), 50 rows.
- **native dialect / struct** — schema inference yields real `id`/`pk`/`mergeOrder`/… columns.
- **datafusion dialect** — cross-container `JOIN` (Cosmos can't do this; DataFusion does), 50 rows.
- **datafusion dialect** — filter pushdown (`WHERE "mergeOrder" > 25`) returns the 25-row subset.
- **connection metadata** — `get_table_types` (`["table"]`), `get_table_schema` (inferred `items`
  columns), and `get_objects` navigated catalog→schema→table→columns (lists the `spikedb` catalog,
  `items`+`categories` containers, and `items` columns).

### Prerequisites

```sh
cargo build -p adbc-cosmos                      # builds target/debug/adbc_cosmos.dll
cargo run  -p cosmos-client --example seed      # seeds the local emulator (spikedb/items+categories)
pip install adbc-driver-manager pyarrow
```

The local Azure Cosmos DB emulator must be running (green tray icon). Uses the well-known
emulator endpoint + key (not secrets).

### Run

```sh
python validation/roundtrip.py
```

Exit code is non-zero if any check fails. Verified 2026-07-01: 15/15 pass
(pyarrow 24.0.0, adbc-driver-manager 1.11.0, Python 3.11).
