import os
import tempfile
from unittest.mock import patch

import pytest
import yaml
from plugins.dags.dag_factory import BaseDAGFactory


class DummyDAGFactory(BaseDAGFactory):
    def create_dag_template(self, **kwargs):
        # Return a dummy DAG object
        return kwargs.get("dag_value", "dummy_dag")


class TestBaseDAGFactory:
    def test_get_configs_reads_yaml(self):
        data = {"foo": "bar"}
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            yaml.dump(data, tmp)
            tmp_path = tmp.name
        try:
            factory = DummyDAGFactory(tmp_path)
            assert factory.configs == data
        finally:
            os.remove(tmp_path)

    def test_abstract_method_raises(self):
        with pytest.raises(TypeError):
            BaseDAGFactory("some_path")

    def test_create_dags_registers_in_globals(self):
        factory = DummyDAGFactory("/dev/null")
        dag_ids = ["dag1", "dag2"]
        fake_globals = {}
        with patch("builtins.globals", return_value=fake_globals):
            factory.create_dags(dag_ids, dag_value="test_dag")
            for dag in dag_ids:
                assert dag in fake_globals
                assert fake_globals[dag] == "test_dag"
