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
from adbc_driver_manager import (
    AdbcConnection,
    AdbcDatabase,
    AdbcStatement,
    GetObjectsDepth,
)

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


def open_database(driver: str) -> AdbcDatabase:
    return AdbcDatabase(
        driver=driver,
        entrypoint=ENTRYPOINT,
        **{
            "adbc.cosmos.endpoint": ENDPOINT,
            "adbc.cosmos.auth": "key",
            "adbc.cosmos.account_key": EMULATOR_KEY,
            "adbc.cosmos.database": DATABASE,
        },
    )


def run_query(driver: str, stmt_opts: dict[str, str], sql: str) -> pyarrow.Table:
    """Load the driver, run one query, return the result as a pyarrow Table."""
    db = open_database(driver)
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


def test_struct_inference_knobs(driver: str) -> None:
    print("[native dialect / struct] inference knobs: decimal + epoch (§3.5)")
    t = run_query(
        driver,
        {
            "adbc.cosmos.dialect": "native",
            "adbc.cosmos.container": "items",
            "adbc.cosmos.output": "struct",
            "adbc.cosmos.number_inference": "decimal",
            "adbc.cosmos.decimal": "20,4",
            "adbc.cosmos.epoch_fields": "_ts:s",
        },
        "SELECT * FROM c",
    )
    fld = {f.name: f.type for f in t.schema}
    check("float field -> decimal128(20,4) survives FFI",
          fld.get("value") == pyarrow.decimal128(20, 4), str(fld.get("value")))
    check("integral field stays int64",
          fld.get("mergeOrder") == pyarrow.int64(), str(fld.get("mergeOrder")))
    check("epoch field -> timestamp[s]",
          fld.get("_ts") == pyarrow.timestamp("s"), str(fld.get("_ts")))


def test_heterogeneous_string(driver: str) -> None:
    print("[native dialect / struct] heterogeneous field -> Utf8 (default build)")
    t = run_query(
        driver,
        {
            "adbc.cosmos.dialect": "native",
            "adbc.cosmos.container": "mixed",
            "adbc.cosmos.output": "struct",
            "adbc.cosmos.heterogeneous": "string",
        },
        "SELECT * FROM c",
    )
    fld = {f.name: f.type for f in t.schema}
    check("type-conflicting 'val' widens to string", fld.get("val") == pyarrow.string(),
          str(fld.get("val")))
    check("4 rows returned", t.num_rows == 4, str(t.num_rows))


def test_metadata(driver: str) -> None:
    print("[connection metadata] get_table_types / get_table_schema / get_objects")
    db = open_database(driver)
    try:
        conn = AdbcConnection(db)
        try:
            # get_table_types  (keep each C handle alive in a local until imported)
            tt_h = conn.get_table_types()
            tt = pyarrow.RecordBatchReader._import_from_c(tt_h.address).read_all()
            check("get_table_types == ['table']",
                  tt.column("table_type").to_pylist() == ["table"],
                  str(tt.to_pylist()))

            # get_table_schema (catalog defaults to current database)
            sch_h = conn.get_table_schema(None, None, "items")
            schema = pyarrow.Schema._import_from_c(sch_h.address)
            check("get_table_schema infers items columns",
                  {"id", "pk", "mergeOrder"}.issubset(set(schema.names)),
                  str(schema.names))

            # get_objects (ALL depth) → navigate catalog → schema → table → columns
            obj_h = conn.get_objects(GetObjectsDepth.ALL, None, None, None, None, None)
            objs = pyarrow.RecordBatchReader._import_from_c(obj_h.address).read_all()
            rows = objs.to_pylist()
            cat = next((r for r in rows if r["catalog_name"] == DATABASE), None)
            check("get_objects lists the database as a catalog", cat is not None,
                  str([r["catalog_name"] for r in rows]))
            tables = [
                t
                for s in (cat["catalog_db_schemas"] or [])
                for t in (s["db_schema_tables"] or [])
            ]
            tnames = {t["table_name"] for t in tables}
            check("get_objects lists items+categories containers",
                  {"items", "categories"}.issubset(tnames), str(sorted(tnames)))
            items = next((t for t in tables if t["table_name"] == "items"), None)
            cols = {c["column_name"] for c in (items["table_columns"] or [])} if items else set()
            check("get_objects lists items columns (id/pk/mergeOrder)",
                  {"id", "pk", "mergeOrder"}.issubset(cols), str(sorted(cols)))
        finally:
            conn.close()
    finally:
        db.close()


def main() -> int:
    driver = find_driver()
    print(f"driver: {driver}\nentrypoint: {ENTRYPOINT}\n")
    for test in (
        test_native_json,
        test_native_struct,
        test_datafusion_join,
        test_datafusion_filter_pushdown,
        test_struct_inference_knobs,
        test_heterogeneous_string,
        test_metadata,
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
