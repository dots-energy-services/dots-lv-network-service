# -*- coding: utf-8 -*-
from datetime import datetime
from esdl import esdl
import helics as h
from dots_infrastructure.DataClasses import EsdlId, HelicsCalculationInformation, PublicationDescription, SubscriptionDescription, TimeStepInformation, TimeRequestType
from dots_infrastructure.HelicsFederateHelpers import HelicsSimulationExecutor
from dots_infrastructure.CalculationServiceHelperFunctions import get_single_param_with_name, get_vector_param_with_name
from dots_infrastructure.Logger import LOGGER
from esdl import EnergySystem

from dss import DSS as dss_engine
import time

class CalculationServiceEConnection(HelicsSimulationExecutor):

    def __init__(self):
        super().__init__()

        subscriptions_values = [
            SubscriptionDescription(esdl_type="PVInstallation", 
                                    input_name="PV_Dispatch", 
                                    input_unit="W", 
                                    input_type=h.HelicsDataType.DOUBLE)
        ]

        publication_values = [
            PublicationDescription(global_flag=True, 
                                   esdl_type="EConnection", 
                                   output_name="EConnectionDispatch", 
                                   output_unit="W", 
                                   data_type=h.HelicsDataType.DOUBLE)
        ]

        e_connection_period_in_seconds = 900

        calculation_information = HelicsCalculationInformation(
            time_period_in_seconds=e_connection_period_in_seconds,
            offset=0, 
            uninterruptible=False, 
            wait_for_current_time_update=False, 
            terminate_on_error=True, 
            calculation_name="load_flow_current_step",
            inputs=[],
            outputs=[],
            calculation_function=self.load_flow_current_step
        )
        self.add_calculation(calculation_information)

        # publication_values = [
        #     PublicationDescription(True, "EConnection", "Schedule", "W", h.HelicsDataType.VECTOR)
        # ]
        #
        # e_connection_period_in_seconds = 21600
        #
        # calculation_information_schedule = HelicsCalculationInformation(e_connection_period_in_seconds, TimeRequestType.PERIOD, 0, False, False, True, "EConnectionSchedule", [], publication_values, self.e_connection_da_schedule)
        # self.add_calculation(calculation_information_schedule)

    def init_calculation_service(self, energy_system: esdl.EnergySystem):
        LOGGER.info("init calculation service")
        for esdl_id in self.simulator_configuration.esdl_ids:
            LOGGER.info(f"Example of iterating over esdl ids: {esdl_id}")

        LOGGER.info('Create Main.dss file')
        f = open("Main.dss", "w+")
        self.network_name = energy_system.name.replace(" ", '_')
        print(self.network_name)
        Voltagebases = []

        f.writelines('Clear \n')
        f.writelines('\nSet DefaultBaseFrequency=50 \n')
        f.writelines('\n! Swing or Source Bar \n')

        for a in energy_system.instance[0].area.asset:
            if isinstance(a, esdl.Import):
                Voltagebases.append(float(a.assetType))
                for port in a.port:
                    busTo = port.connectedTo[0].energyasset
                f.writelines(
                    'New circuit.{network} phases=3 pu=1.0 basekv={Uref} bus1={bus1} \n'.format(
                        network=self.network_name,
                        Uref=a.assetType, bus1=
                        busTo.name.split('Bus')[
                            0]))
        f.writelines('\n! Trafo XFMRCodes \n')
        f.writelines('Redirect XFMRCode.dss \n')
        f.writelines('\n! Trafo \n')

        secondary_trafo_bus = []
        for a in energy_system.instance[0].area.asset:
            # Create the Transformer objects
            if isinstance(a, esdl.Transformer):
                Voltagebases.append(a.voltageSecundary)
                for port in a.port:
                    if isinstance(port, esdl.InPort):
                        busFrom = port.connectedTo[0].energyasset
                    else:
                        busTo = port.connectedTo[0].energyasset
                        secondary_trafo_bus.append(busTo.name.split('Bus')[0])
                f.writelines(
                    'New Transformer.{name} Xfmrcode={type} Buses=[{bus1}  {bus2}.1.2.3] kVs=[{Uprim} {Usecund}] \n'.format(
                        name=a.name, type=a.assetType, bus1=busFrom.name.split('Bus')[0],
                        bus2=busTo.name.split('Bus')[0], Uprim=a.voltagePrimary, Usecund=a.voltageSecundary))

        f.writelines('\n! LineCodes \n')
        f.writelines('Redirect LineCode.dss \n')
        f.writelines('\n! Lines \n')

        for a in energy_system.instance[0].area.asset:
            # Create the Line objects
            if isinstance(a, esdl.ElectricityCable):
                for port in a.port:
                    if isinstance(port, esdl.InPort):
                        busFrom = port.connectedTo[0].energyasset
                    else:
                        busTo = port.connectedTo[0].energyasset
                length = a.length
                linecode = a.assetType
                if busFrom.name.split('Bus')[0] in secondary_trafo_bus:
                    f.writelines('New Line.' + a.name + ' Phases=4 Bus1=' + busFrom.name.split('Bus')[
                        0] + '.1.2.3.0' + ' Bus2=' + busTo.name.split('Bus')[
                                     0] + '.1.2.3.4 LineCode=' + linecode + ' Length=' + str(
                        length) + ' Units=m \n')
                else:
                    f.writelines('New Line.' + a.name + ' Phases=4 Bus1=' + busFrom.name.split('Bus')[
                        0] + '.1.2.3.4' + ' Bus2=' + busTo.name.split('Bus')[
                                     0] + '.1.2.3.4 LineCode=' + linecode + ' Length=' + str(
                        length) + ' Units=m \n')

        f.writelines('\n! Load Definitions \n')
        for a in energy_system.instance[0].area.asset:
            if isinstance(a, esdl.Building):
                for b_a in a.asset:
                    if isinstance(b_a, esdl.EConnection):
                        Busconname = port.connectedTo[0].energyasset
                    if isinstance(b_a, esdl.ElectricityDemand):
                        # if (len(b_a.port)) % 3 == 0:
                        for port in b_a.port:
                            busFrom = port.connectedTo[0].energyasset
                        f.writelines(
                            'New Load.{name}_Ph1 Bus1=Connection{name}.1.4 Phases=1 Conn=wye Model=1 kV=0.230 kW=1 PF=1.0 Vmaxpu=1.5 Vminpu=0.60 \n'.format(
                                name=a.name, bus=busFrom.name[:-4]))
                        f.writelines(
                            'New Load.{name}_Ph2 Bus1=Connection{name}.2.4 Phases=1 Conn=wye Model=1 kV=0.230 kW=1 PF=1.0 Vmaxpu=1.5 Vminpu=0.60 \n'.format(
                                name=a.name, bus=busFrom.name[:-4]))
                        f.writelines(
                            'New Load.{name}_Ph3 Bus1=Connection{name}.3.4 Phases=1 Conn=wye Model=1 kV=0.230 kW=1 PF=1.0 Vmaxpu=1.5 Vminpu=0.60 \n'.format(
                                name=a.name, bus=busFrom.name[:-4]))


        f.writelines('\n! Final Configurations \n')
        f.writelines('Set VoltageBases = {0}\n'.format(Voltagebases))
        f.writelines('CalcVoltageBases\n')

        f.writelines('\n! Solve\n')

        f.writelines('Set mode=snapshot\n')
        f.writelines('! Solve\n')

        f.close()

        f = open("Main.dss", 'r')
        file_contents = f.read()
        print(file_contents)


    def load_flow_current_step(self, param_dict : dict, simulation_time : datetime, time_step_number : TimeStepInformation, esdl_id : EsdlId, energy_system : EnergySystem):
        # ------------------
        # OpenDSS simulation
        # ------------------
        # Define and compile network
        dss_engine.Text.Command = f"compile Main.dss"

        ret_val = {}
        # single_dispatch_value = get_single_param_with_name(param_dict, "PV_Dispatch") # returns the first value in param dict with "PV_Dispatch" in the key name
        # all_dispatch_values = get_vector_param_with_name(param_dict, "PV_Dispatch") # returns all the values as a list in param_dict with "PV_Dispatch" in the key name
        # ret_val["EConnectionDispatch"] = sum(single_dispatch_value)
        # self.influx_connector.set_time_step_data_point(esdl_id, "EConnectionDispatch", simulation_time, ret_val["EConnectionDispatch"])
        return ret_val
    
    # def e_connection_da_schedule(self, param_dict : dict, simulation_time : datetime, time_step_number : TimeStepInformation, esdl_id : EsdlId, energy_system : EnergySystem):
    #     ret_val = {}
    #     return ret_val

if __name__ == "__main__":

    helics_simulation_executor = CalculationServiceEConnection()
    helics_simulation_executor.start_simulation()
    helics_simulation_executor.stop_simulation()
