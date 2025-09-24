from datetime import datetime
import os
import unittest

from esdl import EConnection, EnergySystem, ElectricityCable, Transformer
from lvnetworkservice.lvnetworkservice import CalculationServiceLVNetwork
from dots_infrastructure.DataClasses import SimulatorConfiguration, TimeStepInformation
from dots_infrastructure.test_infra.InfluxDBMock import InfluxDBMock
import helics as h
from esdl.esdl_handler import EnergySystemHandler
from pathlib import Path

from dots_infrastructure import CalculationServiceHelperFunctions


BROKER_TEST_PORT = 23404
START_DATE_TIME = datetime(2024, 1, 1, 0, 0, 0)
SIMULATION_DURATION_IN_SECONDS = 960


def simulator_environment_e_connection():
    return SimulatorConfiguration("EConnection", ["06c59a5e-aa84-4a4d-90db-56fbe4eb266c"], "Mock-Econnection",
                                  "127.0.0.1", BROKER_TEST_PORT, "test-id", SIMULATION_DURATION_IN_SECONDS,
                                  START_DATE_TIME, "test-host", "test-port", "test-username", "test-password",
                                  "test-database-name", h.HelicsLogLevel.DEBUG, ["PVInstallation", "EConnection"])


class Test(unittest.TestCase):

    def setUp(self):
        CalculationServiceHelperFunctions.get_simulator_configuration_from_environment = simulator_environment_e_connection

    def int_service_and_get_energy_system(self, test_file : str) -> tuple[CalculationServiceLVNetwork, EnergySystem]:
        esh = EnergySystemHandler()
        esh.load_file(test_file)
        service = CalculationServiceLVNetwork()
        service.influx_connector = InfluxDBMock()
        service.dss_file_name = "main.dss"
        energy_system = esh.get_energy_system()
        service.init_calculation_service(esh.get_energy_system())
        return service, energy_system
    
    def get_total_active_and_reactive_power(self, service: CalculationServiceLVNetwork, swing_node_trafo_name : str) -> tuple[float, float, float, float]:
        active_circuit = service.dss_engine.ActiveCircuit
        active_power_losses = active_circuit.Losses[0] * 1e-3
        reactive_power_losses = active_circuit.Losses[1] * 1e-3
        active_circuit.SetActiveElement(swing_node_trafo_name)
        transformer_powers = active_circuit.ActiveElement.Powers
        total_active_power_transformer = sum([transformer_powers[0], transformer_powers[2], transformer_powers[4], transformer_powers[6]])
        total_reactive_power_transformer = sum([transformer_powers[1], transformer_powers[3], transformer_powers[5], transformer_powers[7]])
        return total_active_power_transformer, total_reactive_power_transformer, active_power_losses, reactive_power_losses
    
    def tearDown(self):
        os.remove(Path("main.dss"))

    def test_power_flow_input_power_equals_output_power(self):
        pathlist = Path("").glob('**/*.esdl')
        for path in pathlist:
            path_in_str = str(path) 
            with self.subTest(f"Test esdl file {path_in_str}"):
                # Arrange
                service, energy_system = self.int_service_and_get_energy_system(path_in_str)

                params = {}
                econnections = [asset for asset in energy_system.eAllContents() if isinstance(asset, EConnection)]
                for econnection in econnections:
                    params[f"EConnection/aggregated_active_power/{econnection.id}"] = [1000, 1000, 1000]
                    params[f"EConnection/aggregated_reactive_power/{econnection.id}"] = [0, 0, 0]

                # Execute
                ret_val = service.load_flow_current_step(params, datetime(2024, 1, 1), TimeStepInformation(1, 2), "test-id",
                                                         energy_system)

                # Assert
                written_datapoints = service.influx_connector.data_points
                for data_point in written_datapoints:
                    if "home" in data_point.output_name and ".4" not in data_point.output_name:
                        self.assertNotEqual(data_point.value, 0)
    
                total_active_power_params = sum([sum(params[key]) * 1e-3 for key in params.keys() if "aggregated_active_power" in key])
                total_reactive_power_params = sum([sum(params[key]) * 1e-3 for key in params.keys() if "aggregated_reactive_power" in key])
                
                total_active_power, total_reactive_power, active_power_losses, reactive_power_losses = self.get_total_active_and_reactive_power(service, "Transformer.Transformer1")

                self.assertAlmostEqual(total_active_power_params + active_power_losses, total_active_power, delta=1e-1)
                self.assertAlmostEqual(total_reactive_power_params + reactive_power_losses, total_reactive_power, delta=1e-1)


    def test_init_builds_dss_file_correctly(self):
        pathlist = Path("").glob('**/*.esdl')
        for path in pathlist:
            path_in_str = str(path) 
            with self.subTest(f"Test esdl file {path_in_str}"):
                service, energy_system = self.int_service_and_get_energy_system(path_in_str)
                dss_content = []
                with open(service.dss_file_name, "r") as f:
                    dss_content = f.readlines()

                lines_section_start = dss_content.index(service.lines_section_start_marker) + 1
                amount_of_lines_dss = dss_content[lines_section_start:].index("\n")
                trafo_section_start = dss_content.index(service.transformer_section_start_marker) + 1
                amount_of_transformers_dss = dss_content[trafo_section_start:].index("\n")
                load_section_start = dss_content.index(service.load_definition_section_start_marker) + 1
                amount_of_loads_dss = dss_content[load_section_start:].index("\n")

                amount_of_electricity_cables = len([element for element in energy_system.eAllContents() if isinstance(element, ElectricityCable)])
                amount_of_transformers = len([element for element in energy_system.eAllContents() if isinstance(element, Transformer)])
                amount_of_econnections = len([element for element in energy_system.eAllContents() if isinstance(element, EConnection)])

                self.assertEqual(amount_of_lines_dss, amount_of_electricity_cables)
                self.assertEqual(amount_of_transformers, amount_of_transformers_dss)
                self.assertEqual(amount_of_econnections * 3, amount_of_loads_dss)


    def test_correct_values_are_written_to_the_correct_fields(self):
        # Arrange
        service, energy_system = self.int_service_and_get_energy_system("test.esdl")

        params = {}
        econnections = [asset for asset in energy_system.eAllContents() if isinstance(asset, EConnection)]
        for econnection in econnections:
            params[f"EConnection/aggregated_active_power/{econnection.id}"] = [1000, 1000, 1000]
            params[f"EConnection/aggregated_reactive_power/{econnection.id}"] = [0, 0, 0]

        # Execute
        ret_val = service.load_flow_current_step(params, datetime(2024, 1, 1), TimeStepInformation(1, 2), "test-id",
                                                 energy_system)

        # Assert
        data_points_expected_values = {
            "transformer1_limit" : 250.0,
            "cable1" : 39.150000000000006,
            "cable1_limit" : 239.0,
            "connectionhome2.1" : 230.05,
        }
        written_datapoints = service.influx_connector.data_points
        for data_point in written_datapoints:
            if data_point.output_name in data_points_expected_values:
                self.assertAlmostEqual(data_point.value, data_points_expected_values[data_point.output_name], delta=1e-1)



if __name__ == '__main__':
    unittest.main()
