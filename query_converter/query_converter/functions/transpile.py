import pandas as pd
import sqlglot
from sqlglot import exp

from query_converter.functions.helpers import (
    build_column_table_maps,
    build_errors,
    build_mapping_and_comments,
    build_table_errors,
    build_table_map,
    build_table_mapping,
    drop_outdated_errors,
    extract_and_qualify,
    get_cdl_values,
    replace_columns,
    replace_tables,
)
from query_converter.models.query_response import ConvertedQueryResponse


def convert_to_databricks(
    query: str,
    extracted_elements: dict,
    cdl_values_df: pd.DataFrame,
    dialect: str = "tsql",
    catalog: str | None = None,
    mapping_df: pd.DataFrame | None = None,
) -> tuple[str, set, set]:
    """Rewrites a T-SQL query into Databricks SQL using CDL mappings.

    Replaces legacy table and column names with CDL equivalents.
    Preserves table aliases when present for replaced tables.

    Args:
        query: Original T-SQL query.
        extracted_elements: Output of extract_sql_elements().
        cdl_values_df: DataFrame of mapped legacy->CDL columns.

    Returns:
        Databricks SQL query string with updated references.
        Set of replaced columns
    """
    column_map, _ = build_column_table_maps(cdl_values_df)
    table_map = build_table_map(mapping_df)
    tree = sqlglot.parse_one(query, read=dialect)

    replaced_columns = replace_columns(tree, extracted_elements, column_map, catalog)
    replaced_tables = replace_tables(tree, table_map, catalog)

    converted_sql = tree.sql(dialect="databricks")
    return converted_sql, replaced_columns, replaced_tables


def replace_legacy_with_cdl(
    query: str,
    mapping_df: pd.DataFrame,
    dialect: str = "tsql",
    catalog: str | None = None,
):
    """Main pipeline: converts a T-SQL query from legacy to CDL naming.

    Orchestrates extraction, mapping, and SQL rewriting steps to produce
    a Databricks-compatible query along with metadata about mappings,
    comments, and errors.

    Args:
        query: Original T-SQL query string.
        mapping_df: Mapping DataFrame of legacy and CDL names.

    Returns:
        ConvertedQueryResponse with:
            - query: Rewritten Databricks SQL query.
            - comments: Dict of CDL references to associated comments.
            - errors: Dict of unmapped columns with reasons and optional comments.
    """

    extracted_elements = extract_and_qualify(query, mapping_df, dialect)
    cdl_values_df, missing_columns = get_cdl_values(extracted_elements, mapping_df)

    try:
        converted_query, replaced_columns, replaced_tables = convert_to_databricks(
            query, extracted_elements, cdl_values_df, dialect, catalog, mapping_df
        )
    except Exception as e:
        print(f"Conversion failed: {e}")
        converted_query, replaced_columns, replaced_tables = (
            "Unable to parse TSQL query to Databricks",
            set(),
            set(),
        )

    column_mapping, comments = build_mapping_and_comments(cdl_values_df, catalog)
    table_mapping = build_table_mapping(cdl_values_df, catalog)

    errors = build_errors(
        extracted_elements,
        replaced_columns,
        list(column_mapping.keys()),
        missing_columns,
    )
    errors = drop_outdated_errors(errors, converted_query, column_mapping)

    table_errors = build_table_errors(
        extracted_elements=extracted_elements, replaced_tables=replaced_tables
    )

    errors = errors | table_errors

    return ConvertedQueryResponse(
        query=converted_query,
        column_mapping=column_mapping,
        table_mapping=table_mapping,
        comments=comments,
        errors=errors,
    )
