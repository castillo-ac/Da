import pandas as pd
import sqlglot
from sqlglot import exp


def _append_table_name_to_column(parts, table_aliases, from_tables):
    """Append alias or table name to column"""
    if len(parts) > 1 and parts[0] in table_aliases:
        return f"{table_aliases[parts[0]]}.{parts[-1]}"
    if len(parts) == 1 and len(from_tables) == 1:
        return f"{from_tables[0]}.{parts[0]}"
    return ".".join(parts)


def _resolve_chain(ref, fwd):
    """Alias/cte column name with the base name (without alias)."""
    seen = set()
    cur = ref
    while isinstance(cur, str) and cur in fwd and cur not in seen:
        seen.add(cur)
        cur = fwd[cur]
    return cur


def _invert_to_base_list(fwd):
    """Build base -> [aliases] map"""
    collapsed = {k: _resolve_chain(v, fwd) for k, v in fwd.items()}
    rev = {}
    for alias, base in collapsed.items():
        if not isinstance(base, str):
            continue
        rev.setdefault(base, [])
        if alias not in rev[base]:
            rev[base].append(alias)
    return rev


def _merge(acc, part):
    acc["columns"].update(part["columns"])
    acc["tables"].update(part["tables"])
    acc["schemas"].update(part["schemas"])
    acc["databases"].update(part["databases"])
    acc["table_aliases"].update(part["table_aliases"])
    acc["forward_map"].update(part["forward_map"])


def _parse_statement(tree, dialect: str = "tsql") -> dict:
    tables, schemas, databases = set(), set(), set()
    table_aliases = {}
    columns = set()
    fwd = {}  # forward map: alias-ish -> base/expression

    # tables + aliases in this subtree
    for t in tree.find_all(exp.Table):
        parts = []
        if t.args.get("catalog"):
            databases.add(t.args["catalog"].name.lower())
            parts.append(t.args["catalog"].name.lower())
        if t.args.get("db"):
            schemas.add(t.args["db"].name.lower())
            parts.append(t.args["db"].name.lower())
        parts.append(t.name.lower())
        full = ".".join(parts)
        tables.add(full)
        if t.args.get("alias"):
            table_aliases[t.args["alias"].name.lower()] = full

    from_tables = list(tables)

    # regular column refs in this subtree (apply alias + 1-table rule)
    for c in tree.find_all(exp.Column):
        parts = [p.name.lower() for p in c.parts]
        raw = ".".join(parts)
        fq = _append_table_name_to_column(parts, table_aliases, from_tables)
        if fq != raw:
            fwd[raw] = fq
        columns.add(fq)

    # if this subtree is a CTE, map cte_alias.output_col -> base/expression
    if isinstance(tree, exp.CTE) and tree.args.get("alias"):
        cte_alias = tree.args["alias"].name.lower()
        body = tree.this
        if isinstance(body, exp.Subquery):
            body = body.this
        if isinstance(body, exp.Select):
            for sel in body.expressions or []:
                if isinstance(sel, exp.Alias):
                    out = sel.alias_or_name.lower()
                    if isinstance(sel.this, exp.Column):
                        parts = [p.name.lower() for p in sel.this.parts]
                        fwd[f"{cte_alias}.{out}"] = _append_table_name_to_column(
                            parts, table_aliases, from_tables
                        )
                    else:
                        fwd[f"{cte_alias}.{out}"] = sel.this.sql(
                            dialect=dialect
                        ).lower()
                elif isinstance(sel, exp.Column):
                    out = sel.parts[-1].name.lower()
                    parts = [p.name.lower() for p in sel.parts]
                    fwd[f"{cte_alias}.{out}"] = _append_table_name_to_column(
                        parts, table_aliases, from_tables
                    )

    return {
        "columns": columns,
        "tables": tables,
        "schemas": schemas,
        "databases": databases,
        "table_aliases": table_aliases,
        "forward_map": fwd,
    }


def extract_sql_elements(sql: str, dialect: str = "tsql") -> dict:
    """Parses a T-SQL query and extracts structural SQL elements.

    Extracted elements include fully qualified tables, schemas, databases,
    table aliases, column aliases, and subquery aliases. Aliases are resolved
    where possible but preserved in output.

    Args:
        sql: Input T-SQL query string.

    Returns:
        Dictionary containing:
            - columns: List of fully qualified column references (excluding aliases).
            - tables: List of fully qualified table names.
            - schemas: List of schema names.
            - databases: List of database names.
            - table_aliases: Mapping of alias name -> fully qualified table name.
            - column_aliases: Mapping of alias name -> underlying column/expression.
    """

    tree = sqlglot.parse_one(sql, read=dialect)

    result = {
        "columns": set(),
        "tables": set(),
        "schemas": set(),
        "databases": set(),
        "table_aliases": {},
        "forward_map": {},
    }

    # each CTE has to be parsed separately to make sure we catch all the columns
    for cte in tree.find_all(exp.CTE):
        _merge(result, _parse_statement(cte, dialect))

    if isinstance(tree, exp.Union):
        for branch in [tree.left, tree.right]:
            _merge(result, _parse_statement(branch, dialect))

    _merge(result, _parse_statement(tree, dialect))

    # build reverse resolution map keyed by fully resolved base
    column_resolution = _invert_to_base_list(result["forward_map"])
    del result["forward_map"]

    # finalize types
    result["columns"] = list(result["columns"])
    result["tables"] = list(result["tables"])
    result["schemas"] = list(result["schemas"])
    result["databases"] = list(result["databases"])
    result["column_aliases"] = column_resolution

    return result


def find_cdl_values(
    extracted_elements: dict, mapping_df: pd.DataFrame
) -> tuple[pd.DataFrame, list[dict]]:
    """Matches legacy SQL columns to CDL equivalents using a mapping file.

    Uses extracted SQL elements to match against mapping DataFrame entries
    for table, schema, and column names. Returns mapped results and a list of
    unmapped columns.

    Args:
        extracted_elements: Output of extract_sql_elements().
        mapping_df: Mapping DataFrame containing legacy and CDL columns.

    Returns:
        A tuple of:
            - DataFrame of successfully mapped legacy->CDL column matches.
            - List of unmapped column dictionaries with optional comments.
    """
    results = []
    unmapped_columns = []

    # Iterate over each base column
    for base_col, aliases in extracted_elements["column_aliases"].items():
        # Split base_col into parts
        parts = [p.strip("`[] ") for p in base_col.split(".")]
        db_name = schema_name = table_name = column_name = None

        if len(parts) == 4:
            db_name, schema_name, table_name, column_name = parts
        elif len(parts) == 3:
            schema_name, table_name, column_name = parts
        elif len(parts) == 2:
            table_name, column_name = parts
        else:
            column_name = parts[-1]

        # Resolve table aliases (if any)
        if table_name:
            table_name = extracted_elements["table_aliases"].get(table_name, table_name)

        # Try mapping each alias including base
        all_refs = [base_col] + aliases
        matched = False
        for ref in all_refs:
            # Split ref into parts for db/schema/table/column
            ref_parts = [p.strip("`[] ") for p in ref.split(".")]
            ref_db = ref_schema = ref_table = ref_col = None
            if len(ref_parts) == 4:
                ref_db, ref_schema, ref_table, ref_col = ref_parts
            elif len(ref_parts) == 3:
                ref_schema, ref_table, ref_col = ref_parts
            elif len(ref_parts) == 2:
                ref_table, ref_col = ref_parts
            else:
                ref_col = ref_parts[-1]

            # Filter mapping
            match_df = mapping_df[
                (mapping_df["Legacy column"].str.lower() == (ref_col or "").lower())
                & (
                    (
                        mapping_df["Legacy table"].str.lower()
                        == (ref_table or "").lower()
                    )
                    if ref_table
                    else True
                )
            ]
            if ref_schema:
                match_df = match_df[
                    match_df["Legacy schema"].str.lower() == ref_schema.lower()
                ]
            if ref_db:
                match_df = match_df[match_df["Legacy db"].str.lower() == ref_db.lower()]

            if match_df.empty:
                continue

            # Deduplicate & pick first valid mapping
            match_df = match_df.sort_values(
                "CDL-STC column", na_position="last"
            ).drop_duplicates(subset=["Legacy column", "Legacy table"], keep="first")
            row = match_df.iloc[0]

            cdl_col = row.get("CDL-STC column")
            if not cdl_col or str(cdl_col).strip() in ("", "-"):
                continue

            results.append(
                {
                    "Legacy Column": ref_col,
                    "Legacy Table": ref_table,
                    "Legacy Schema": ref_schema,
                    "Legacy DB": ref_db,
                    "CDL-STC Column": row.get("CDL-STC column"),
                    "CDL-STC Schema": row.get("CDL-STC schema"),
                    "CDL-STC Table": row.get("CDL-STC table"),
                    "Comment": row.get("Comment"),
                }
            )
            break

    expected_columns = [
        "Legacy Column",
        "Legacy Table",
        "Legacy Schema",
        "Legacy DB",
        "CDL-STC Column",
        "CDL-STC Schema",
        "CDL-STC Table",
        "Comment",
    ]

    mapped_df = pd.DataFrame(results)
    if mapped_df.empty:
        mapped_df = pd.DataFrame(columns=expected_columns)
    else:
        mapped_df = mapped_df[expected_columns]

    valid_df = mapped_df[
        mapped_df["CDL-STC Column"].notna()
        & (mapped_df["CDL-STC Column"].str.strip() != "")
        & (mapped_df["CDL-STC Column"].str.strip() != "-")
    ]
    invalid_df = mapped_df[~mapped_df.index.isin(valid_df.index)]

    for _, row in invalid_df.iterrows():
        unmapped_columns.append(
            {
                "Legacy DB": row.get("Legacy DB"),
                "Legacy Schema": row.get("Legacy Schema"),
                "Legacy Table": row.get("Legacy Table"),
                "Legacy Column": row.get("Legacy Column"),
                "Comment": row.get("Comment"),
                "error": "CDL mapping missing",
            }
        )

    return valid_df, unmapped_columns


def qualify_unmapped_columns(extracted_elements: dict, mapping_df: pd.DataFrame):
    """
    For columns that are not yet in column_aliases (keys or values),
    check which detected table contains them in the mapping. If exactly
    one table matches, add as fully qualified column -> itself in column_aliases.
    """
    detected_tables = extracted_elements["tables"]
    resolved_columns = set(extracted_elements["column_aliases"].keys()) | set(
        col for vals in extracted_elements["column_aliases"].values() for col in vals
    )

    for col in extracted_elements["columns"]:
        if col in resolved_columns:
            continue  # already handled
        parts = col.split(".")
        if len(parts) == 1:  # unqualified
            matches = []
            for tbl in detected_tables:
                tbl_parts = tbl.split(".")
                tbl_name = tbl_parts[-1]
                if not mapping_df[
                    (mapping_df["Legacy table"].str.lower() == tbl_name.lower())
                    & (mapping_df["Legacy column"].str.lower() == col.lower())
                ].empty:
                    matches.append(tbl_name)
            if len(matches) == 1:
                fq_col = f"{matches[0]}.{col}"
                extracted_elements["column_aliases"][fq_col] = [col]
    return extracted_elements
