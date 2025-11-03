import json
import re

import sqlparse
from pandas import DataFrame


def pretty_print_sql(sql: str) -> str:
    """Reindent SQL query and convert keywords to uppercase."""
    formatted_sql = sqlparse.format(sql, reindent=True, keyword_case="upper")
    return formatted_sql


def column_mapping_to_table(mapping: dict) -> str:
    """Converts a dictionary of legacy-to-CDL mappings into an HTML table.

    Each dictionary key is treated as the legacy column (DB.Schema.Table.Column),
    and its value is treated as the mapped CDL column.

    Args:
        mapping: Dictionary of {legacy_column: cdl_column}.

    Returns:
        HTML table string representing the mapping data.
    """
    rows = "".join(
        f"<tr><td>{legacy}</td><td>{cdl}</td></tr>" for legacy, cdl in mapping.items()
    )
    return (
        "<table>"
        "<thead><tr><th>Legacy Column</th><th>CDL-STC Column</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def table_mapping_to_table(mapping: dict) -> str:
    """Converts a dictionary of legacy-to-CDL table mappings into an HTML table."""
    rows = "".join(
        f"<tr><td>{legacy}</td><td>{cdl}</td></tr>" for legacy, cdl in mapping.items()
    )
    return (
        "<table>"
        "<thead><tr><th>Legacy Table</th><th>CDL-STC Table</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def comment_to_table(d: dict) -> str:
    """Converts a dictionary of comments into an HTML table.

    Each dictionary key is treated as a full column name, and its corresponding value
    is displayed as the comment.

    Args:
        d: Mapping of column names to comments.

    Returns:
        HTML table string representing the comment data.
    """
    rows = "".join(
        f"<tr><td>{key}</td><td>{value if not isinstance(value, dict) else json.dumps(value)}</td></tr>"
        for key, value in d.items()
    )
    return f"<table><thead><tr><th>Column</th><th>Comment</th></tr></thead><tbody>{rows}</tbody></table>"


def error_to_table(errors: dict) -> str:
    """Converts a dictionary of errors into an HTML table.

    Each dictionary key represents an identifier (such as a column or schema name).
    The value must be another dictionary containing:
      - 'error': Error message.
      - 'comment': (Optional) In case error it's related to column this field contain a Comment value from mapping file .
    """

    rows = ""
    for k, v in errors.items():
        key_val = k
        error_val = v.get("error", "")
        error_type = v.get("error_type", "").upper()
        comment_val = v.get("comment", "") if v is None or v != "nan" else ""
        if isinstance(key_val, dict):
            key_val = json.dumps(key_val)
        if isinstance(error_val, dict):
            error_val = json.dumps(error_val)
        if isinstance(comment_val, dict):
            comment_val = json.dumps(comment_val)

        rows += f"<tr><td>{error_type}</td><td>{key_val}</td><td>{error_val}</td><td>{comment_val}</td></tr>"

    return f"<table><thead><tr><th>Type</th><th>Name</th><th>Error</th><th>Comment</th></tr></thead><tbody>{rows}</tbody></table>"


def full_mapping_to_table(df: DataFrame) -> str:
    """Convert mapping DataFrame into HTML table"""

    # Define the fixed column order
    column_order = [
        "Legacy db",
        "Legacy schema",
        "Legacy table",
        "Legacy column",
        "CDL-STC schema",
        "CDL-STC table",
        "CDL-STC column",
        "Comment",
    ]

    header = "".join(f"<th>{col}</th>" for col in column_order)
    rows = ""
    for _, row in df.iterrows():
        cells = "".join(f"<td>{row[col]}</td>" for col in column_order)
        rows += f"<tr>{cells}</tr>"

    return f"""
    <table id="mapping-table" class="display">
        <thead><tr>{header}</tr></thead>
        <tbody>{rows}</tbody>
    </table>
    """


def highlight_error_columns(sql: str, error_columns: list[str]) -> str:
    """
    Highlight SQL columns that appear in error_columns.
    Only compares the column name, preserves original SQL text.
    """
    error_column_names = {col.split(".")[-1].lower() for col in error_columns}

    word_pattern = re.compile(r"\b\w+\b")

    alias_pattern = re.compile(r"\bAS\s+(`?\w+`?|\"?\w+\"?)", re.IGNORECASE)

    alias_names = {m.group(1).strip('`"').lower() for m in alias_pattern.finditer(sql)}

    def replacer(match):
        word = match.group(0)
        if word.lower() in error_column_names and word.lower() not in alias_names:
            return f'<span class="error-column">{word}</span>'
        return word

    highlighted_sql = word_pattern.sub(replacer, sql)

    return highlighted_sql


def highlight_error_tables(sql: str, error_tables: list[str]) -> str:
    """
    Highlight tables in SQL from the given list.
    Does not highlight if followed by a dot (indicating a column).
    """

    # Sort tables by descending length to match longest first (avoid partial matches)
    sorted_tables = sorted(error_tables, key=len, reverse=True)

    highlighted_sql = sql

    for table in sorted_tables:
        # Split table parts by dot
        parts = table.split(".")
        # Build a regex that allows optional backticks/quotes around each part
        part_patterns = [rf"`?{re.escape(p)}`?" for p in parts]
        table_pattern = r"\.".join(part_patterns)

        # Lookahead to avoid .column_name
        pattern = re.compile(rf"\b{table_pattern}\b(?!\.)", re.IGNORECASE)

        # Replace matches with highlight span
        highlighted_sql = pattern.sub(
            lambda m: f'<span class="error-column">{m.group(0)}</span>', highlighted_sql
        )

    return highlighted_sql


def highlight_sql_errors(
    sql: str, error_columns: list[str], error_tables: list[str]
) -> str:
    """
    Highlight both error columns and error tables in the SQL.
    Calls the existing highlight_error_columns and highlight_error_tables functions.
    """
    highlighted = highlight_error_tables(sql, error_tables)
    highlighted = highlight_error_columns(highlighted, error_columns)
    return highlighted
