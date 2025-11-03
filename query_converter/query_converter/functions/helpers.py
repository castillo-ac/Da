import re

import pandas as pd
import sqlglot
from sqlglot import exp

from query_converter.functions.data_profiling import (
    extract_sql_elements,
    find_cdl_values,
    qualify_unmapped_columns,
)


def remove_sql_comments(query: str) -> str:
    """Strip single-line comments."""
    return re.sub(r"--.*?$", "", query, flags=re.MULTILINE)


def build_column_table_maps(cdl_values_df: pd.DataFrame):
    column_map = {
        (
            row["Legacy Schema"] or "",
            row["Legacy Table"] or "",
            row["Legacy Column"] or "",
        ): (row["CDL-STC Schema"], row["CDL-STC Table"], row["CDL-STC Column"])
        for _, row in cdl_values_df.iterrows()
    }

    table_map = {
        (row["Legacy Schema"] or "", row["Legacy Table"] or ""): (
            row["CDL-STC Schema"],
            row["CDL-STC Table"],
        )
        for _, row in cdl_values_df.drop_duplicates(
            subset=["Legacy Schema", "Legacy Table"]
        ).iterrows()
    }
    return column_map, table_map


def build_table_map(mapping_df: pd.DataFrame):
    """
    Builds Legacy (schema, table) -> CDL (schema, table) mapping.

    - Normalizes case to lower
    - Replaces NaN with ""
    - Deduplicates by (Legacy schema, Legacy table)
      preferring rows where CDL-STC schema/table are non-empty.
    """
    df = mapping_df.fillna("")

    df = df.sort_values(
        by=["CDL-STC schema", "CDL-STC table"],
        ascending=[False, False],  # non-empty strings sort after ""
    ).drop_duplicates(subset=["Legacy schema", "Legacy table"], keep="first")

    table_map = {
        (str(row["Legacy schema"]).lower(), str(row["Legacy table"]).lower()): (
            str(row["CDL-STC schema"]),
            str(row["CDL-STC table"]),
        )
        for _, row in df.iterrows()
    }

    return table_map


def replace_columns(tree, extracted_elements, column_map, new_catalog):
    replaced_columns = set()

    # Replace columns
    for col in tree.find_all(exp.Column):
        original_parts = [p.name for p in col.parts]  # keep original casing for alias
        orig_len = len(original_parts)

        # Determine column and table for lookup (lowercased)
        lookup_parts = [p.name.lower() for p in col.parts]
        db_name = schema_name = table_name = column_name = None
        if orig_len == 4:
            db_name, schema_name, table_name, column_name = lookup_parts
        elif orig_len == 3:
            schema_name, table_name, column_name = lookup_parts
        elif orig_len == 2:
            table_name, column_name = lookup_parts
        else:
            column_name = lookup_parts[-1]

        lookup_schema = schema_name
        lookup_table = table_name

        # resolve aliases for lookup only
        if table_name and table_name in extracted_elements.get("table_aliases", {}):
            real_table = extracted_elements["table_aliases"][table_name]
            real_parts = real_table.split(".")
            if len(real_parts) == 3:
                db_name, lookup_schema, lookup_table = real_parts
            elif len(real_parts) == 2:
                lookup_schema, lookup_table = real_parts
            else:
                lookup_table = real_parts[0]

        key = (lookup_schema or "", lookup_table or "", column_name or "")

        if key not in column_map:
            candidates = []
            k_schema, k_table, k_col = key
            if k_schema and k_table:
                candidates = [
                    k
                    for k in column_map
                    if k[0] == k_schema and k[1] == k_table and k[2] == k_col
                ]
            if not candidates and k_table:
                candidates = [
                    k for k in column_map if k[1] == k_table and k[2] == k_col
                ]
            if not candidates and not k_table and not k_schema:
                candidates = [k for k in column_map if k[2] == k_col]
            if len(candidates) == 1:
                key = candidates[0]

        if key in column_map:
            new_schema, new_table, new_column = column_map[key]
            col.set("this", exp.to_identifier(new_column.strip()))

            if table_name:
                col.set(
                    "table",
                    exp.to_identifier(original_parts[-2]) if orig_len >= 2 else None,
                )
                col.set("db", None)
            else:
                col.set(
                    "table", exp.to_identifier(new_table) if orig_len >= 2 else None
                )
                col.set("db", exp.to_identifier(new_schema) if orig_len >= 3 else None)

            # Replace catalog if present
            if col.args.get("catalog") and new_catalog:
                old_catalog = col.args["catalog"].name
                if isinstance(new_catalog, dict):
                    col.set(
                        "catalog",
                        exp.to_identifier(new_catalog.get(old_catalog, old_catalog)),
                    )
                else:
                    col.set("catalog", exp.to_identifier(new_catalog))

            replaced_columns.add(
                ".".join(
                    filter(None, [db_name, lookup_schema, lookup_table, column_name])
                )
            )

            resolved_parts = [
                col.args.get("db").name if col.args.get("db") else None,
                col.args.get("table").name if col.args.get("table") else None,
                col.name,
            ]
            resolved_name = ".".join(filter(None, resolved_parts))
            replaced_columns.add(resolved_name)
    return replaced_columns


def replace_tables(
    tree: exp.Expression, table_map: dict, table_catalog: str | None
) -> set:
    """Mutates the tree in-place, replacing tables using table_map."""
    replaced_tables = set()

    for table in tree.find_all(exp.Table):
        catalog = (
            table.args.get("catalog").name.lower()
            if table.args.get("catalog")
            else None
        )
        db = table.args.get("db")
        schema_name = db.name.lower() if db else None
        table_name = table.name.lower()

        # Standard lookup: schema + table
        key = ((schema_name or "").lower(), table_name.lower())

        if key not in table_map:
            # Fallback: try first key that matches table only
            candidates = [k for k in table_map if k[1] == table_name.lower()]
            key = candidates[0] if candidates else None

        if key:
            new_schema, new_table = table_map[key]
            if new_table:
                table.set("this", exp.to_identifier(new_table))
                replaced_tables.add(
                    ".".join(filter(None, [catalog, schema_name, table_name]))
                )
            if new_schema:
                table.set("db", exp.to_identifier(new_schema))
            if (
                new_schema
                and new_table
                and table_catalog
                and table_catalog.strip() != ""
            ):
                table.set("catalog", exp.to_identifier(table_catalog))

    return replaced_tables


def extract_and_qualify(
    query: str, mapping_df: pd.DataFrame, dialect: str = "tsql"
) -> dict:
    try:
        extracted = extract_sql_elements(query, dialect)
        extracted = qualify_unmapped_columns(extracted, mapping_df)
        return extracted
    except Exception as e:
        print(f"Extraction or qualification failed: {e}")
        return {
            "columns": [],
            "tables": [],
            "schemas": [],
            "databases": [],
            "table_aliases": {},
            "column_aliases": {},
        }


def get_cdl_values(
    extracted_elements: dict, mapping_df: pd.DataFrame
) -> tuple[pd.DataFrame, list]:
    try:
        return find_cdl_values(extracted_elements, mapping_df)
    except Exception as e:
        print(f"CDL value mapping failed: {e}")
        return pd.DataFrame(), []


def build_mapping_and_comments(
    cdl_values_df: pd.DataFrame, catalog: str | None
) -> tuple[dict, dict]:
    mapping = {}
    comments = {}
    if not catalog or catalog.strip() == "":
        catalog = ""
    else:
        catalog = f"{catalog}."

    for _, row in cdl_values_df.iterrows():
        try:
            legacy_parts = [
                row.get("Legacy DB"),
                row.get("Legacy Schema"),
                row.get("Legacy Table"),
                row.get("Legacy Column"),
            ]
            legacy_key = ".".join(str(part) for part in legacy_parts if pd.notna(part))

            cdl_parts = [
                row.get("CDL-STC Schema"),
                row.get("CDL-STC Table"),
                row.get("CDL-STC Column"),
            ]
            cdl_val = ".".join(str(part) for part in cdl_parts if pd.notna(part))
            mapping[legacy_key] = f"{catalog}{cdl_val}"

            key = f"{catalog}" + ".".join(
                str(part) for part in cdl_parts if pd.notna(part)
            )
            if isinstance(row.get("Comment"), str) and row["Comment"].strip() not in (
                "-",
                "",
            ):
                comments[key] = row["Comment"]

        except Exception as e:
            print(f"Failed to process row: {e}")

    return mapping, comments


def build_errors(
    extracted_elements: dict,
    replaced_columns: set,
    mapped_columns: list,
    missing_columns: list,
) -> dict[str, dict]:
    errors = {}
    try:
        all_columns = set(extracted_elements.get("column_aliases", []))
        missing_after_replacement = all_columns - replaced_columns - set(mapped_columns)
        for col in missing_after_replacement:
            errors[col] = {
                "error_type": "column",
                "error": "Not found in the mapping file",
                "original_column": col,
            }
    except Exception as e:
        print(f"Failed to compute missing columns: {e}")

    for row in missing_columns:
        try:
            legacy_key = ".".join(
                str(p)
                for p in [
                    row.get("Legacy DB"),
                    row.get("Legacy Schema"),
                    row.get("Legacy Table"),
                    row.get("Legacy Column"),
                ]
                if p and str(p).strip() != ""
            )
            errors[legacy_key] = {
                "error_type": "column",
                "error": row.get("error", "CDL mapping missing"),
                "original_column": legacy_key,
                "comment": row.get("Comment") if pd.notna(row.get("Comment")) else "",
            }
        except Exception as e:
            print(f"Failed to process missing column row: {e}")
    return errors


def drop_outdated_errors(
    errors: dict, query: str, column_mapping: dict | None = None
) -> dict:
    """
    Remove errors for columns that:
      1. No longer exist in the query text
      2. Or have been successfully mapped

    Args:
        errors: Original errors dict.
        query: SQL query text.
        column_mapping: Optional dict of successfully mapped columns.
    """
    pruned_errors = {}
    lower_query = query.lower()
    mapped_keys = {k.lower() for k in (column_mapping or {})}

    for col, info in errors.items():
        col_name = col.split(".")[-1].lower()
        col_lower = col.lower()

        if "*" in col_name or col_lower in mapped_keys:
            continue

        if (
            f" {col_name}" in lower_query
            or f".{col_name}" in lower_query
            or f"({col_name}" in lower_query
            or f"`{col_name}" in lower_query
        ):
            pruned_errors[col] = info

    return pruned_errors


def build_table_errors(
    extracted_elements: dict,
    replaced_tables: set | None = None,
) -> dict[str, dict]:
    errors = {}
    try:
        all_tables = set(extracted_elements.get("tables", []))
        unmapped_tables = all_tables - (replaced_tables or set())

        for tbl in unmapped_tables:
            errors[tbl] = {
                "error_type": "table",
                "error": "Table not found in the mapping file",
                "original_table": tbl,
            }
    except Exception as e:
        print(f"Failed to compute missing tables: {e}")
    return errors


def build_table_mapping(cdl_values_df: pd.DataFrame, catalog: str | None) -> dict:
    """Builds a mapping of legacy tables -> CDL tables, with catalog appended if given."""
    table_mapping = {}
    if not catalog or catalog.strip() == "":
        catalog = ""
    else:
        catalog = f"{catalog}."

    for _, row in cdl_values_df.drop_duplicates(
        subset=["Legacy DB", "Legacy Schema", "Legacy Table"]
    ).iterrows():
        try:
            legacy_parts = [
                str(part)
                for part in [
                    row.get("Legacy DB"),
                    row.get("Legacy Schema"),
                    row.get("Legacy Table"),
                ]
                if pd.notna(part) and str(part).strip() != ""
            ]
            legacy_key = ".".join(legacy_parts)

            cdl_parts = [
                str(part)
                for part in [
                    row.get("CDL-STC Schema"),
                    row.get("CDL-STC Table"),
                ]
                if pd.notna(part) and str(part).strip() != ""
            ]
            cdl_val = ".".join(cdl_parts)

            if cdl_val:
                table_mapping[legacy_key] = f"{catalog}{cdl_val}"
        except Exception as e:
            print(f"Failed to process table row: {e}")

    return table_mapping
