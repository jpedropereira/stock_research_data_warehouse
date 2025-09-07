import os
import tempfile
from unittest.mock import patch

import plugins.dags.dag_factory as dag_factory_module
import pytest
import yaml
from plugins.dags.dag_factory import BaseDAGFactory


class DummyDAGFactory(BaseDAGFactory):
    def create_dag_template(self, **kwargs):
        # Return a dummy DAG object
        return kwargs.get("dag_value", "dummy_dag")

    def dag_arguments_generator(self, **kwargs):
        yield {"dag_id": "dag1", "dag_value": "dummy_dag1"}
        yield {"dag_id": "dag2", "dag_value": "dummy_dag2"}


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
        fake_globals = {}
        with patch("builtins.globals", return_value=fake_globals):
            factory.create_dags([])  # dag_ids is ignored in new logic
            assert fake_globals["dag1"] == "dummy_dag1"
            assert fake_globals["dag2"] == "dummy_dag2"

    def test_dag_arguments_generator_yields_expected(self):
        factory = DummyDAGFactory("/dev/null")
        args = list(factory.dag_arguments_generator())
        assert len(args) == 2
        assert args[0]["dag_id"] == "dag1"
        assert args[1]["dag_id"] == "dag2"

    def test_create_master_dag_registers_master_and_triggers(self):
        factory = DummyDAGFactory("/dev/null")
        master_dag_id = "test_master_dag"
        dags_ids = ["dag1", "dag2"]

        # Remove if already present in the dag_factory module namespace
        if hasattr(dag_factory_module, master_dag_id):
            delattr(dag_factory_module, master_dag_id)

        try:
            factory.create_master_dag(master_dag_id, dags_ids)
            # Assert it's registered in the dag_factory module's global namespace
            assert hasattr(dag_factory_module, master_dag_id)
            master_dag = getattr(dag_factory_module, master_dag_id)
            # Check tasks exist
            task_ids = [t.task_id for t in master_dag.tasks]
            for dag_id in dags_ids:
                assert f"trigger_{dag_id}" in task_ids
            assert "start_task" in task_ids
            assert "end_task" in task_ids
        finally:
            if hasattr(dag_factory_module, master_dag_id):
                delattr(dag_factory_module, master_dag_id)
