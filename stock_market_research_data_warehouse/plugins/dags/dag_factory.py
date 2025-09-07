from abc import ABC, abstractmethod

import yaml
from airflow import DAG


class BaseDAGFactory(ABC):
    """
    Abstract base class for DAG factory implementations.

    Inherit from this class to create a factory that generates Airflow DAGs from configuration files.
    Subclasses must implement the `create_dag_template` method, which defines the logic for creating a single DAG.

    Args:
        configs_path (str): Path to a YAML file containing configuration for the DAGs to be created.

    Usage:
        1. Inherit from BaseDAGFactory.
        2. Implement the `create_dag_template` method in your subclass. The implementation must return
           a DAG object.
        3. Use `create_dags` to register all DAGs in the global namespace for Airflow discovery.
        4. Optionally, implement `create_master_dag` to create a DAG that triggers all generated DAGs.
    """

    def __init__(self, configs_path: str):
        self.configs = self.get_configs(configs_path)

    @staticmethod
    def get_configs(configs_path: str) -> dict:
        """
        Load and parse the YAML configuration file.

        Args:
            configs_path (str): Path to the YAML file.

        Returns:
            dict: Parsed configuration dictionary.
        """
        with open(configs_path) as f:
            configs = yaml.safe_load(f)
        return configs

    @abstractmethod
    def create_dag_template(self, **kwargs) -> DAG:
        """
        Abstract method to define the logic for creating a single DAG.

        Subclasses must implement this method to return a DAG object based on provided arguments.

        Args:
            **kwargs: Arbitrary keyword arguments required for DAG creation.

        Returns:
            DAG: The created DAG object.
        """
        raise NotImplementedError(
            "create_dag_template() needs to be implemented for every subclass of BaseDAGFactory."
        )

    def create_dags(self, dags_ids: list, **kwargs) -> None:
        """
        Create and register multiple DAGs in the global namespace.

        Args:
            dags_ids (list): List of DAG IDs (strings) to create and register.
            **kwargs: Additional keyword arguments passed to `create_dag_template`.

        This method will call `create_dag_template` for each DAG ID and register the resulting DAG
        in the global namespace, making it discoverable by Airflow.
        """
        for dag in dags_ids:
            globals()[dag] = self.create_dag_template(**kwargs)

    def create_master_dag(self, dags_ids: list) -> None:
        """
        (To be implemented) Create a master DAG that triggers all DAGs created by this factory.

        Args:
            dags_ids (list): List of DAG IDs to be included in the master DAG.

        Returns:
            None
        """
        # TODO: implement method to create a DAG running all the dags created by the factory
