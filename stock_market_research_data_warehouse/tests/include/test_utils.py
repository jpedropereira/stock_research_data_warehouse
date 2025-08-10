import os
import tempfile
from contextlib import contextmanager

import pytest
import yaml
from include.utils import load_table_config, render_ddl


class BaseTestUtils:
    test_yaml_content = """
tables:
  test_table:
    ddl: |
      CREATE TABLE IF NOT EXISTS {{schema}}.test_table (
          id INT,
          name TEXT
      );
    columns:
      id:
        csv_col: id
        df_dtype: int
      name:
        csv_col: name
        df_dtype: str
"""

    @classmethod
    def create_temp_yaml(cls, content):
        fd, path = tempfile.mkstemp(suffix=".yaml")
        with os.fdopen(fd, "w") as tmp:
            tmp.write(content)
        return path

    @classmethod
    @contextmanager
    def temp_yaml(cls, content):
        path = cls.create_temp_yaml(content)
        try:
            yield path
        finally:
            os.remove(path)


class TestLoadTableConfig(BaseTestUtils):
    def test_load_table_config(self):
        with self.temp_yaml(self.test_yaml_content) as yaml_path:
            columns = load_table_config(yaml_path, "test_table")
            assert isinstance(columns, dict)
            assert "id" in columns
            assert columns["id"]["csv_col"] == "id"
            assert columns["id"]["df_dtype"] == "int"
            assert columns["name"]["csv_col"] == "name"
            assert columns["name"]["df_dtype"] == "str"

    def test_load_table_config_missing_table(self):
        with self.temp_yaml(self.test_yaml_content) as yaml_path:
            with pytest.raises(ValueError):
                load_table_config(yaml_path, "missing_table")


class TestRenderDdl(BaseTestUtils):
    def test_render_ddl(self):
        with self.temp_yaml(self.test_yaml_content) as yaml_path:
            ddl = render_ddl(yaml_path, "test_table", schema_name="myschema")
            assert "CREATE TABLE IF NOT EXISTS myschema.test_table" in ddl
            assert "id INT" in ddl
            assert "name TEXT" in ddl

    def test_render_ddl_missing_table(self):
        with self.temp_yaml(self.test_yaml_content) as yaml_path:
            with pytest.raises(ValueError):
                render_ddl(yaml_path, "missing_table", schema_name="myschema")

    def test_render_ddl_missing_ddl(self):
        # Remove ddl from YAML
        broken_yaml = yaml.safe_load(self.test_yaml_content)
        del broken_yaml["tables"]["test_table"]["ddl"]
        fd, path = tempfile.mkstemp(suffix=".yaml")
        with os.fdopen(fd, "w") as tmp:
            yaml.dump(broken_yaml, tmp)
        try:
            with pytest.raises(ValueError):
                render_ddl(path, "test_table", schema_name="myschema")
        finally:
            os.remove(path)
