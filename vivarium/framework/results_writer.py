"""Provides a class for consistently managing and writing vivarium outputs and output paths."""
from collections import defaultdict
import os
from datetime import datetime

import yaml


class ResultsWriter:
    """Writes output files for vivarium simulations.

    Attributes
    ----------
    results_root: str
        The root directory to which results will be written.
    """

    def __init__(self, results_root):
        """
        Parameters
        ----------
        results_root: str
            The root directory to which results will be written.
        """
        self.results_root = results_root
        os.makedirs(results_root, exist_ok=True)
        self._directories = defaultdict(lambda: self.results_root)

    def add_sub_directory(self, key, path):
        """Adds a sub-directory to the results directory.

        Parameters
        ----------
        key: str
            A look-up key for the directory path.
        path: str
            The relative path from the root of the results directory to the sub-directory.

        Returns
        -------
        str:
            The absolute path to the sub-directory.
        """
        sub_dir_path = os.path.join(self.results_root, path)
        os.makedirs(sub_dir_path, exist_ok=True)
        self._directories[key] = sub_dir_path
        return sub_dir_path

    def write_output(self, data, file_name, key=None):
        """Writes output data to disk.

        Parameters
        ----------
        data: pandas.DataFrame or dict
            The data to write to disk.
        file_name: str
            The name of the file to write.
        key: str, optional
            The lookup key for the sub_directory to write results to, if any.
        """
        path = os.path.join(self._directories[key], file_name)
        extension = file_name.split('.')[-1]

        if extension == 'yaml':
            with open(path, 'w') as f:
                yaml.dump(data, f)
        elif extension == 'hdf':
            data.to_hdf(path, 'data', format='table')
        else:
            raise NotImplementedError(
                f"Only 'yaml' and 'hdf' file types are supported. You requested {extension}")

    def dump_simulation_configuration(self, component_configuration_path):
        """Sets up a simulation to get the complete configuration, then writes it to disk.

        Parameters
        ----------
        component_configuration_path: str
            Absolute path to a yaml file with the simulation component configuration.
        """
        from vivarium.framework.engine import build_simulation_configuration, load_component_manager, setup_simulation
        configuration = build_simulation_configuration({'components': component_configuration_path})
        configuration.run_configuration.results_directory = self.results_root
        component_manager = load_component_manager(configuration)
        setup_simulation(component_manager, configuration)
        self.write_output(configuration.to_dict(), 'base_config.yaml')
        with open(component_configuration_path) as f:
            self.write_output(f.read(), 'components.yaml')


def get_results_writer(results_directory, component_configuration_file):
    launch_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    config_name = os.path.basename(component_configuration_file.rpartition('.')[0])
    results_root = results_directory + f"/{config_name}/{launch_time}"
    return ResultsWriter(results_root)


def get_results_writer_for_restart(results_directory):
    return ResultsWriter(results_directory)
