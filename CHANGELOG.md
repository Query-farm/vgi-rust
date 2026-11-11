# Changelog

## Unreleased

- **Protocol 3.3.0** (`VGI_PROTOCOL_VERSION`). Requires the matching VGI
  extension (peers must agree on major + minor); upgrade workers and the
  extension together.
- `FunctionInfo.visibility` (`vgi_protocol::protocol::enums::function_visibility`:
  `"public"` / `"table_only"`; null means public) and `FunctionInfo::is_table_only`.
- Every function a catalog table names (scan, time-travel and branch scans,
  INSERT / UPDATE / DELETE) is always listed, so DuckDB has its capabilities.
  **User-visible:** write functions, and table scans embedded in the table
  (`CatTable::with_function` / `scan_function_impl`), disappear from
  `duckdb_functions()` and are no longer callable directly from SQL, unless the
  table sets the new `CatTable::expose_scan` (`CatTable::with_expose_scan`). A
  scan the author also registers with `Worker::register_table` stays public.
  Write functions are always `table_only`.
- `Worker::hide_function` / `Dispatcher::hide_function` now list the function
  as `table_only` instead of omitting it; for table functions they are no
  longer needed. New `CatalogModel::table_only_function_names`.
