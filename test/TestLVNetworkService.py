from datetime import datetime
import os
import unittest

from esdl import EConnection, EnergySystem, ElectricityCable, Joint
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
    
    # def tearDown(self):
    #     os.remove("Main.dss")

    # def test_example(self):
    #     # Arrange
    #     service, energy_system = self.int_service_and_get_energy_system("test.esdl")

    #     params = {}
    #     params['EConnection/aggregated_active_power/5c19dcff-b004-4644-99b9-f42d15a34f3a'] = [2000, 2000, 2000]
    #     params['EConnection/aggregated_active_power/1412f71f-a9d2-4c66-a834-385cf91c3767'] = [1000, 1000, 1000]
    #     params['EConnection/aggregated_active_power/bb93de79-0d9e-4cf2-8794-ebac2d238f45'] = [2000, 2000, 2000]

    #     params['EConnection/aggregated_reactive_power/5c19dcff-b004-4644-99b9-f42d15a34f3a'] = [2000, 2000, 2000]
    #     params['EConnection/aggregated_reactive_power/1412f71f-a9d2-4c66-a834-385cf91c3767'] = [1000, 1000, 1000]
    #     params['EConnection/aggregated_reactive_power/bb93de79-0d9e-4cf2-8794-ebac2d238f45'] = [2000, 2000, 2000]

    #     # Execute
    #     ret_val = service.load_flow_current_step(params, datetime(2024, 1, 1), TimeStepInformation(1, 2), "test-id",
    #                                              energy_system)

    #     # Assert
    #     written_datapoints = service.influx_connector.data_points

    #     expected_names = [f"cable{i}" for i in range(1, 9)] + ["transformer1"] + [f"connectionhome{i}.{j}" for i in range(1, 4) for j in range(1, 5)] + [f"node{i}.{j}" for i in range(1, 11) for j in range(1, 5)]
    #     expected_names.remove("node10.4")
    #     expected_names.remove("node1.4")
    #     written_value_names = [data_point.output_name for data_point in written_datapoints]
    #     for expected_name in expected_names:
    #         self.assertIn(expected_name, written_value_names)

    def test_mv_lv_example(self):
        # Arrange
        service, energy_system = self.int_service_and_get_energy_system("test/mv-energy-system.esdl")

        params = {}
        econnections = [asset for asset in energy_system.eAllContents() if isinstance(asset, EConnection)]
        for econnection in econnections:
            params[f"EConnection/aggregated_active_power/{econnection.id}"] = [2000, 2000, 2000]
            params[f"EConnection/aggregated_reactive_power/{econnection.id}"] = [2000, 2000, 2000]

        # Execute
        ret_val = service.load_flow_current_step(params, datetime(2024, 1, 1), TimeStepInformation(1, 2), "test-id",
                                                 energy_system)

        econnection_names = [element.name.lower().replace(".", "_") for element in energy_system.eAllContents() if isinstance(element, EConnection)]
        electricity_cable_names = [element.name.lower().replace(".", "_") for element in energy_system.eAllContents() if isinstance(element, ElectricityCable)]
        joint_names = [element.name.lower().replace(".", "_") for element in energy_system.eAllContents() if isinstance(element, Joint)]
        expected_names = []
        joint_names.remove("jointHighVoltageTrafo".lower())
        for name in econnection_names:
            for i in range(1,4):
                expected_names.append(f"{name}.{i}")
        for name in joint_names:
            if "joint" in name:
                for i in range(1,4):
                    expected_names.append(f"{name}.{i}")
            elif "lv_node" in name:
                for i in range(1,5):
                    expected_names.append(f"{name}.{i}")
        expected_names.extend(electricity_cable_names)

        # Make the cut
        # check values
        # reduce output values

        # Assert
        # written_datapoints = service.influx_connector.data_points
        # written_value_names = [data_point.output_name for data_point in written_datapoints]
        # for expected_name in expected_names:
        #     if expected_name not in written_value_names:
        #         print(f"Expected name '{expected_name}' not found in written value names.")
        #         self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
