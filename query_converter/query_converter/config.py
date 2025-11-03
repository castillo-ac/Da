import importlib.resources as pkg_resources

import pandas as pd


def load_static_file(filename: str, as_text: bool = True):
    """Loads a static file packaged within the 'query_converter.static' module.

    Args:
        filename: Name of the file to load (e.g., 'style.css', 'converter_template.html', 'STC_Mapping.xlsx').
        as_text: If True, read the file as text (UTF-8).
                        If False, read as binary.

    Returns:
        str or bytes: File content, depending on as_text.
    """
    if as_text:
        with pkg_resources.open_text("query_converter.static", filename) as f:
            return f.read()
    else:
        with pkg_resources.open_binary("query_converter.static", filename) as f:
            return f.read()


def load_template(name: str):
    return load_static_file(name, as_text=True)


def load_css(name="style.css"):
    return load_static_file(name, as_text=True)


def load_mapping(path: str | None = None) -> pd.DataFrame:
    """Load the CDL mapping file as a pandas DataFrame.

    Args:
        path: External path to a mapping file. If provided, this file is loaded instead.

    Returns:
        pd.DataFrame: Loaded DataFrame or empty DataFrame if loading fails.
    """
    required_columns = {
        "Legacy db",
        "Legacy schema",
        "Legacy table",
        "Legacy column",
        "CDL-STC schema",
        "CDL-STC table",
        "CDL-STC column",
        "Comment",
    }

    try:
        if path is None:
            raise ValueError("No path provided for the mapping file.")

        df = pd.read_excel(path)

        # Validate required columns (extras allowed)
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"Mapping file is missing required columns: {missing}")

        return df[list(required_columns)]

    except FileNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(f"Error loading mapping file: {e}") from e
