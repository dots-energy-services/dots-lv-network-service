from datetime import datetime
import os
import unittest

from esdl import EConnection, EnergySystem, ElectricityCable, Joint, Transformer
from lvnetworkservice.lvnetworkservice import CalculationServiceLVNetwork
from dots_infrastructure.DataClasses import SimulatorConfiguration, TimeStepInformation
from dots_infrastructure.test_infra.InfluxDBMock import InfluxDBMock
import helics as h
from esdl.esdl_handler import EnergySystemHandler

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
        energy_system = esh.get_energy_system()
        service.init_calculation_service(esh.get_energy_system())
        return service, energy_system
    
    def get_total_active_and_reactive_power(self, service: CalculationServiceLVNetwork, swing_node_trafo_name : str) -> tuple[float, float]:
        active_circuit = service.dss_engine.ActiveCircuit
        active_power_losses = active_circuit.Losses[0] * 1e-3
        reactive_power_losses = active_circuit.Losses[1] * 1e-3
        active_circuit.SetActiveElement(swing_node_trafo_name)
        transformer_powers = active_circuit.ActiveElement.Powers
        total_active_power_transformer = sum([transformer_powers[0], transformer_powers[2], transformer_powers[4], transformer_powers[6]])
        total_reactive_power_transformer = sum([transformer_powers[1], transformer_powers[3], transformer_powers[5], transformer_powers[7]])
        return total_active_power_transformer - active_power_losses, total_reactive_power_transformer - reactive_power_losses
    
    # def tearDown(self):
    #     os.remove("main.dss")

    lv_test_example_files = [
        # {"file_name" : "test/test.esdl", "lines_correction" : 0},
        {"file_name" : "test/trafo1_lvnetwork9.esdl"}
    ]

    def test_example(self):
        for param_dict in self.init_test_example_files:
            with self.subTest(f"Test esdl file {param_dict['file_name']}"):
                # Arrange
                service, energy_system = self.int_service_and_get_energy_system(param_dict['file_name'])
        
                params = {}
                params['EConnection/aggregated_active_power/5c19dcff-b004-4644-99b9-f42d15a34f3a'] = [1000, 2000, 3000]
                params['EConnection/aggregated_active_power/1412f71f-a9d2-4c66-a834-385cf91c3767'] = [1000, 2000, 3000]
                params['EConnection/aggregated_active_power/bb93de79-0d9e-4cf2-8794-ebac2d238f45'] = [1000, 2000, 3000]
        
                params['EConnection/aggregated_reactive_power/5c19dcff-b004-4644-99b9-f42d15a34f3a'] = [0, 0, 0]
                params['EConnection/aggregated_reactive_power/1412f71f-a9d2-4c66-a834-385cf91c3767'] = [0, 0, 0]
                params['EConnection/aggregated_reactive_power/bb93de79-0d9e-4cf2-8794-ebac2d238f45'] = [0, 0, 0]
        
                # Execute
                ret_val = service.load_flow_current_step(params, datetime(2024, 1, 1), TimeStepInformation(1, 2), "test-id",
                                                         energy_system)
        
                # Assert
                written_datapoints = service.influx_connector.data_points
                
                expected_names = [f"cable{i}" for i in range(1, 9)] + ["transformer1"] + [f"connectionhome{i}.{j}" for i in range(1, 4) for j in range(1, 5)]
                written_value_names = [data_point.output_name for data_point in written_datapoints]
                for expected_name in expected_names:
                    self.assertIn(expected_name, written_value_names)
                    if "connectionhome" in expected_name:
                        val = next(val for val in written_datapoints if val.output_name == expected_name)
                        self.assertNotEqual(val, 0)
        
                total_active_power_params = sum([sum(params[key]) * 1e-3 for key in params.keys() if "aggregated_active_power" in key])
                total_reactive_power_params = sum([sum(params[key]) * 1e-3 for key in params.keys() if "aggregated_reactive_power" in key])
                
                total_active_power, total_reactive_power = self.get_total_active_and_reactive_power(service, "Transformer.Transformer1")
        
                self.assertAlmostEqual(total_active_power_params, total_active_power, delta=1e-2)
                self.assertAlmostEqual(total_reactive_power_params, total_reactive_power, delta=1e-2)


    init_test_example_files = [
        # {"file_name" : "test/test.esdl", "lines_correction" : 0},
        # {"file_name" : "test/mv-energy-system.esdl", "lines_correction" : 1}
        {"file_name" : "test/trafo1_lvnetwork9.esdl", "lines_correction" : 0}
    ]

    def test_init_builds_dss_file_correctly(self):
        for param_dict in self.init_test_example_files:
            with self.subTest(f"Test esdl file {param_dict['file_name']}"):
                service, energy_system = self.int_service_and_get_energy_system(param_dict['file_name'])
                dss_content = []
                with open("main.dss", "r") as f:
                    dss_content = f.readlines()

                lines_section_start = dss_content.index(service.lines_section_start_marker) + 1
                amount_of_lines_dss = dss_content[lines_section_start:].index("\n")
                trafo_section_start = dss_content.index(service.transformer_section_start_marker) + 1
                amount_of_transformers_dss = dss_content[trafo_section_start:].index("\n")
        
                amount_of_electricity_cables = len([element for element in energy_system.eAllContents() if isinstance(element, ElectricityCable)])
                amount_of_transformers = len([element for element in energy_system.eAllContents() if isinstance(element, Transformer)])

                self.assertEqual(amount_of_lines_dss, amount_of_electricity_cables - param_dict['lines_correction'])
                self.assertEqual(amount_of_transformers, amount_of_transformers_dss)

    # def test_mv_lv_example(self):
    #     # Arrange
    #     service, energy_system = self.int_service_and_get_energy_system("test/mv-energy-system.esdl")

    #     params = {}
    #     econnections = [asset for asset in energy_system.eAllContents() if isinstance(asset, EConnection)]
    #     for econnection in econnections:
    #         params[f"EConnection/aggregated_active_power/{econnection.id}"] = [1000, 1000, 1000]
    #         params[f"EConnection/aggregated_reactive_power/{econnection.id}"] = [0, 0, 0]

    #     # Execute
    #     ret_val = service.load_flow_current_step(params, datetime(2024, 1, 1), TimeStepInformation(1, 2), "test-id",
    #                                              energy_system)

    #     written_datapoints = service.influx_connector.data_points
    #     for data_point in written_datapoints:
    #         if "home" in data_point.output_name and ".4" not in data_point.output_name:
    #             self.assertNotEqual(data_point.value, 0)

    #     total_active_power, total_reactive_power = self.get_total_active_and_reactive_power(service, "Transformer.HighVoltageTrafo")

    #     total_active_power_params = sum([sum(params[key]) * 1e-3 for key in params.keys() if "aggregated_active_power" in key])
    #     total_reactive_power_params = sum([sum(params[key]) * 1e-3 for key in params.keys() if "aggregated_reactive_power" in key])

    #     self.assertAlmostEqual(total_active_power_params, total_active_power, delta=1e-2)
    #     self.assertAlmostEqual(total_reactive_power_params, total_reactive_power, delta=1e-2)

if __name__ == '__main__':
    unittest.main()
