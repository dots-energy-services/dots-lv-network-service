from datetime import datetime
import unittest
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
        esh = EnergySystemHandler()
        esh.load_file('test.esdl')
        energy_system = esh.get_energy_system()
        self.energy_system = energy_system

    def test_example(self):
        # Arrange
        service = CalculationServiceLVNetwork()
        service.influx_connector = InfluxDBMock()
        params = {}
        params['EConnection/aggregated_active_power/5c19dcff-b004-4644-99b9-f42d15a34f3a'] = [2000, 2000, 2000]
        params['EConnection/aggregated_active_power/1412f71f-a9d2-4c66-a834-385cf91c3767'] = [1000, 1000, 1000]
        params['EConnection/aggregated_active_power/bb93de79-0d9e-4cf2-8794-ebac2d238f45'] = [2000, 2000, 2000]

        params['EConnection/aggregated_reactive_power/5c19dcff-b004-4644-99b9-f42d15a34f3a'] = [2000, 2000, 2000]
        params['EConnection/aggregated_reactive_power/1412f71f-a9d2-4c66-a834-385cf91c3767'] = [1000, 1000, 1000]
        params['EConnection/aggregated_reactive_power/bb93de79-0d9e-4cf2-8794-ebac2d238f45'] = [2000, 2000, 2000]

        service.init_calculation_service(self.energy_system)

        # Execute
        ret_val = service.load_flow_current_step(params, datetime(2024, 1, 1), TimeStepInformation(1, 2), "test-id",
                                                 self.energy_system)

        # Assert
        written_datapoints = service.influx_connector.data_points

        expected_names = [f"cable{i}" for i in range(1, 9)] + ["transformer1"] + [f"connectionhome{i}.{j}" for i in range(1, 4) for j in range(1, 5)] + [f"node{i}.{j}" for i in range(1, 11) for j in range(1, 5)]
        expected_names.remove("node10.4")
        expected_names.remove("node1.4")
        written_value_names = [data_point.output_name for data_point in written_datapoints]
        for expected_name in expected_names:
            self.assertIn(expected_name, written_value_names)

if __name__ == '__main__':
    unittest.main()
