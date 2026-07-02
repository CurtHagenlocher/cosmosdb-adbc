#!/usr/bin/env python3
"""End-to-end ADBC round-trip validation for the Cosmos driver's C ABI (cdylib).

Unlike the in-process Rust tests, this loads the built `adbc_cosmos` cdylib through
`adbc_driver_manager` exactly as a real Python/pyarrow consumer would, exercising the
full FFI/ABI boundary: driver init, options, `execute_query`, and the Arrow C Data
Interface (including `arrow.json` extension-type passthrough into pyarrow).

Prereqs:
  - `cargo build -p adbc-cosmos`   (produces target/debug/adbc_cosmos.dll)
  - local Cosmos emulator running + seeded:
      cargo run -p cosmos-client --example seed
  - pip install adbc-driver-manager pyarrow

Run:  python validation/roundtrip.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow
from adbc_driver_manager import AdbcConnection, AdbcDatabase, AdbcStatement

# Public, well-known emulator key (not a secret).
EMULATOR_KEY = (
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
)
ENDPOINT = "https://localhost:8081/"
DATABASE = "spikedb"
ENTRYPOINT = "AdbcDriverCosmosDbInit"


def find_driver() -> str:
    root = Path(__file__).resolve().parent.parent
    for rel in ("target/debug/adbc_cosmos.dll", "target/release/adbc_cosmos.dll"):
        p = root / rel
        if p.exists():
            return str(p)
    sys.exit(f"driver cdylib not found under {root}/target — run `cargo build -p adbc-cosmos`")


def run_query(driver: str, stmt_opts: dict[str, str], sql: str) -> pyarrow.Table:
    """Load the driver, run one query, return the result as a pyarrow Table."""
    db = AdbcDatabase(
        driver=driver,
        entrypoint=ENTRYPOINT,
        **{
            "adbc.cosmos.endpoint": ENDPOINT,
            "adbc.cosmos.auth": "key",
            "adbc.cosmos.account_key": EMULATOR_KEY,
            "adbc.cosmos.database": DATABASE,
        },
    )
    try:
        conn = AdbcConnection(db)
        try:
            stmt = AdbcStatement(conn)
            try:
                if stmt_opts:
                    stmt.set_options(**stmt_opts)
                stmt.set_sql_query(sql)
                stream, _rows = stmt.execute_query()
                reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
                return reader.read_all()
            finally:
                stmt.close()
        finally:
            conn.close()
    finally:
        db.close()


# ── test cases ────────────────────────────────────────────────────────────────

PASSED = 0
FAILED = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASSED, FAILED
    if cond:
        PASSED += 1
        print(f"  PASS: {name}")
    else:
        FAILED += 1
        print(f"  FAIL: {name} — {detail}")


def ext_name(field: pyarrow.Field) -> str | None:
    """Extension name of a field, whether pyarrow promoted it to a real ExtensionType
    (canonical `arrow.json` → `JsonType`) or left it as storage + field metadata."""
    name = getattr(field.type, "extension_name", None)
    if name:
        return name
    meta = field.metadata or {}
    v = meta.get(b"ARROW:extension:name")
    return v.decode() if v is not None else None


def test_native_json(driver: str) -> None:
    print("[native dialect / json output] SELECT * FROM c ORDER BY c.mergeOrder")
    t = run_query(
        driver,
        {"adbc.cosmos.dialect": "native", "adbc.cosmos.container": "items"},
        "SELECT * FROM c ORDER BY c.mergeOrder",
    )
    check("single column", t.num_columns == 1, f"got {t.num_columns}")
    check("column named 'document'", t.schema.field(0).name == "document",
          t.schema.field(0).name)
    check("arrow.json extension survives FFI",
          ext_name(t.schema.field(0)) == "arrow.json",
          f"ext={ext_name(t.schema.field(0))}")
    check("50 rows", t.num_rows == 50, f"got {t.num_rows}")


def test_native_struct(driver: str) -> None:
    print("[native dialect / struct output] inferred columns")
    t = run_query(
        driver,
        {
            "adbc.cosmos.dialect": "native",
            "adbc.cosmos.container": "items",
            "adbc.cosmos.output": "struct",
        },
        "SELECT * FROM c ORDER BY c.mergeOrder",
    )
    names = set(t.schema.names)
    check("inferred real columns (id/pk/mergeOrder present)",
          {"id", "pk", "mergeOrder"}.issubset(names), str(sorted(names)))
    check("50 rows", t.num_rows == 50, f"got {t.num_rows}")


def test_datafusion_join(driver: str) -> None:
    print("[datafusion dialect] cross-container JOIN")
    t = run_query(
        driver,
        {"adbc.cosmos.dialect": "datafusion"},
        "SELECT i.name AS item_name, c.label AS category "
        "FROM items i JOIN categories c ON i.pk = c.pk",
    )
    check("columns item_name/category", set(t.schema.names) == {"item_name", "category"},
          str(t.schema.names))
    check("50 joined rows", t.num_rows == 50, f"got {t.num_rows}")


def test_datafusion_filter_pushdown(driver: str) -> None:
    print('[datafusion dialect] filter pushdown: WHERE "mergeOrder" > 25')
    t = run_query(
        driver,
        {"adbc.cosmos.dialect": "datafusion"},
        'SELECT id, "mergeOrder" FROM items WHERE "mergeOrder" > 25',
    )
    check("25-row subset", t.num_rows == 25, f"got {t.num_rows}")
    col = t.column("mergeOrder").combine_chunks()
    all_gt = all(v.as_py() > 25 for v in col)
    check("every row satisfies mergeOrder > 25", all_gt, "a row leaked past the filter")


def main() -> int:
    driver = find_driver()
    print(f"driver: {driver}\nentrypoint: {ENTRYPOINT}\n")
    for test in (
        test_native_json,
        test_native_struct,
        test_datafusion_join,
        test_datafusion_filter_pushdown,
    ):
        try:
            test(driver)
        except Exception as e:  # noqa: BLE001 — surface any FFI/driver error as a failure
            global FAILED
            FAILED += 1
            print(f"  FAIL: {test.__name__} raised {type(e).__name__}: {e}")
        print()
    print(f"=== {PASSED} passed, {FAILED} failed ===")
    return 1 if FAILED else 0


if __name__ == "__main__":
    raise SystemExit(main())
