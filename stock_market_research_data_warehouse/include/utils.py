import yaml
from jinja2 import Template


def load_table_config(column_mapping_yaml_path: str, table_name: str) -> dict:
    """Load column mapping for a table from a YAML config file."""
    with open(column_mapping_yaml_path) as f:
        mappings = yaml.safe_load(f)
    table_cfg = mappings["tables"].get(table_name)
    if not table_cfg:
        raise ValueError(
            f"Table config for '{table_name}' is missing in '{column_mapping_yaml_path}'."
        )
    columns = table_cfg.get("columns")
    if not isinstance(columns, dict) or len(columns) == 0:
        raise ValueError(
            f"Column mapping for table '{table_name}' is missing or empty in '{column_mapping_yaml_path}'."
        )
    # Lowercase all keys and csv_col values
    columns = {
        k.lower(): {"csv_col": v["csv_col"].lower(), "df_dtype": v["df_dtype"]}
        for k, v in columns.items()
    }
    return columns


def render_ddl(column_mapping_yaml_path: str, table_name: str, schema_name) -> str:
    """Load ddl for a given table"""
    with open(column_mapping_yaml_path) as f:
        mappings = yaml.safe_load(f)
    table_cfg = mappings["tables"].get(table_name)
    if table_cfg is None:
        raise ValueError(f"Table '{table_name}' not found in YAML config.")
    ddl = table_cfg.get("ddl")
    if not ddl:
        raise ValueError(f"No DDL found for table '{table_name}' in YAML config.")
    template = Template(ddl)
    return template.render(schema=schema_name)
