from dataclasses import dataclass


@dataclass
class ConvertedQueryResponse:
    query: str
    column_mapping: dict
    table_mapping: dict
    comments: dict
    errors: dict
