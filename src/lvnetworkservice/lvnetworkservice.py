# -*- coding: utf-8 -*-
from datetime import datetime
from esdl import esdl
import helics as h
from dots_infrastructure.DataClasses import EsdlId, HelicsCalculationInformation, SubscriptionDescription, TimeStepInformation
from dots_infrastructure.HelicsFederateHelpers import HelicsSimulationExecutor
from dots_infrastructure.Logger import LOGGER
from esdl import EnergySystem

from dss import DSS as dss_engine
import math


class CalculationServiceLVNetwork(HelicsSimulationExecutor):

    def __init__(self):
        super().__init__()

        subscriptions_values = [
            SubscriptionDescription(esdl_type="EConnection",
                                    input_name="aggregated_active_power",
                                    input_unit="W", 
                                    input_type=h.HelicsDataType.VECTOR),
            SubscriptionDescription(esdl_type="EConnection",
                                    input_name="aggregated_reactive_power",
                                    input_unit="W",
                                    input_type=h.HelicsDataType.VECTOR)
        ]

        e_connection_period_in_seconds = 900

        calculation_information = HelicsCalculationInformation(
            time_period_in_seconds=e_connection_period_in_seconds,
            offset=0, 
            uninterruptible=False, 
            wait_for_current_time_update=False, 
            terminate_on_error=True, 
            calculation_name="load_flow_current_step",
            inputs=subscriptions_values,
            outputs=[],
            calculation_function=self.load_flow_current_step
        )
        self.add_calculation(calculation_information)

    def init_calculation_service(self, energy_system: esdl.EnergySystem):
        LOGGER.info("init calculation service")

        self.ems_list = []

        LOGGER.info('Create Main.dss file')
        f = open("Main.dss", "w+")
        self.network_name = energy_system.name.replace(" ", '_')
        LOGGER.debug(self.network_name)
        Voltagebases = []

        f.writelines('Clear \n')
        f.writelines('\nSet DefaultBaseFrequency=50 \n')
        f.writelines('\n! Swing or Source Bar \n')

        import_count = 0

        for a in energy_system.instance[0].area.asset:
            if isinstance(a, esdl.Import):
                Voltagebases.append(float(a.assetType))
                import_count += 1
                for port in a.port:
                    busTo = port.connectedTo[0].energyasset
                if import_count == 1:
                    f.writelines(
                        'New circuit.{network} phases=3 pu=1.0 basekv={Uref} bus1={bus1} \n'.format(
                            network='{0}_{1}'.format(self.network_name,import_count),
                            Uref=a.assetType, bus1=
                            busTo.name.split('Bus')[
                                0]))
                else:
                    f.writelines(
                        'New Vsource.{network} phases=3 pu=1.0 basekv={Uref} bus1={bus1} \n'.format(
                            network='{0}_{1}'.format(self.network_name, import_count),
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
                        self.ems_list.append(b_a.id)
                    if isinstance(b_a, esdl.ElectricityDemand):
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

        file_contents = f.read()
        f.close()
        LOGGER.debug(file_contents)

    def load_flow_current_step(self, param_dict : dict, simulation_time : datetime, time_step_number : TimeStepInformation, esdl_id : EsdlId, energy_system : EnergySystem):
        # START user calc
        LOGGER.info("calculation 'load_flow_current_step' started")

        # ------------------
        # OpenDSS simulation
        # ------------------
        # Define and compile network
        LOGGER.debug('OpenDSS compile network')
        dss_engine.Text.Command = f"compile Main.dss"

        totalactiveload = 0
        totalreactiveload = 0
        # Receive load values from EMS and adjust load values:

        LOGGER.debug('OpenDSS add loads to network')
        connection = 0

        dss_engine.ActiveCircuit.Loads.First
        while connection < len(self.ems_list):
            # Determine the number of phases for the current connection
            num_phases = len(param_dict['EConnection/aggregated_active_power/{0}'.format(self.ems_list[connection])])
            # Loop through phases for the current connection
            phase = 0
            while phase < num_phases:
                dss_engine.ActiveCircuit.Loads.kW = param_dict['EConnection/aggregated_active_power/{0}'.format(self.ems_list[connection])][phase] * 1e-3
                totalactiveload += param_dict['EConnection/aggregated_active_power/{0}'.format(self.ems_list[connection])][phase] * 1e-3
                LOGGER.debug('Active power {0} {1}'.format(dss_engine.ActiveCircuit.Loads.Name,
                      dss_engine.ActiveCircuit.Loads.kW))

                dss_engine.ActiveCircuit.Loads.kvar = param_dict['EConnection/aggregated_reactive_power/{0}'.format(self.ems_list[connection])][phase] * 1e-3
                totalreactiveload += param_dict['EConnection/aggregated_reactive_power/{0}'.format(self.ems_list[connection])][phase] * 1e-3
                LOGGER.debug('Reactive power {0} {1}'.format(dss_engine.ActiveCircuit.Loads.Name,
                             dss_engine.ActiveCircuit.Loads.kva))

                dss_engine.ActiveCircuit.Loads.Next
                phase += 1
            connection += 1

        # Solve load flow calculation
        LOGGER.debug('OpenDSS solve loadflow calculation')
        dss_engine.ActiveCircuit.Solution.Solve()

        # Process results
        BusVoltageMag = []
        LineCurrentMag = []
        LineCurrentAng = []
        TotalLineCurrentMag = []
        TotalLineCurrentLim = []
        TransformerPower = []
        TransformerPowerLim = []

        # Phase voltage magnitudes for each bus:
        LOGGER.debug('Extract voltages')
        for i in range(len(dss_engine.ActiveCircuit.AllBusVmag)):
            BusVoltageMag.append(round(dss_engine.ActiveCircuit.AllBusVmag[i], 2))

        # Phase current magnitudes and angles for each line:
        LOGGER.debug('Extract current magnitudes and angles')
        for l in range(dss_engine.ActiveCircuit.Lines.Count):
            dss_engine.ActiveCircuit.SetActiveElement(
                'Line.{0}'.format(dss_engine.ActiveCircuit.Lines.AllNames[l]))
            Total_line_current = 0
            for i in range(1, 4):
                LineCurrentMag.append(
                    round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2], 2))
                LineCurrentAng.append(
                    round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2 + 1], 2))
                Total_line_current += round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2],
                                            2)
            TotalLineCurrentMag.append(Total_line_current)
            TotalLineCurrentLim.append(float(dss_engine.ActiveCircuit.ActiveCktElement.NormalAmps))

        # Apparent power for each transformer:
        LOGGER.debug('Extract apparent power for each transformer')
        dss_engine.ActiveCircuit.Transformers.First
        for t in range(dss_engine.ActiveCircuit.Transformers.Count):
            dss_engine.ActiveCircuit.SetActiveElement(
                'Transformer.{0}'.format(dss_engine.ActiveCircuit.Transformers.AllNames[t]))
            TransformerPower.append(round(math.sqrt(dss_engine.ActiveCircuit.ActiveElement.TotalPowers[0] ** 2 +
                                                    dss_engine.ActiveCircuit.ActiveElement.TotalPowers[1] ** 2), 2))

            TransformerPowerLim.append((dss_engine.ActiveCircuit.Transformers.kVA))
            dss_engine.ActiveCircuit.Transformers.Next
        LineLimitNames = [x + '_limit' for x in dss_engine.ActiveCircuit.Lines.AllNames]
        TransformerLimitNames = [x + '_limit' for x in dss_engine.ActiveCircuit.Transformers.AllNames]

        ret_val = {}

        # Write results to influxdb
        LOGGER.debug('Write results to influxdb')
        for d in range(len(dss_engine.ActiveCircuit.AllNodeNames)):
            self.influx_connector.set_time_step_data_point(esdl_id, dss_engine.ActiveCircuit.AllNodeNames[d],
                                                          simulation_time, BusVoltageMag[d])
            LOGGER.debug('{0}: {1} V'.format(dss_engine.ActiveCircuit.AllNodeNames[d], BusVoltageMag[d]))
        for d in range(len(dss_engine.ActiveCircuit.Lines.AllNames)):
            self.influx_connector.set_time_step_data_point(esdl_id, dss_engine.ActiveCircuit.Lines.AllNames[d],
                                                          simulation_time, TotalLineCurrentMag[d])
            self.influx_connector.set_time_step_data_point(esdl_id, LineLimitNames[d], simulation_time,
                                                          TotalLineCurrentLim[d])
            LOGGER.debug('{0}: {1} A, limit={2} A'.format(dss_engine.ActiveCircuit.Lines.AllNames[d], TotalLineCurrentMag[d], TotalLineCurrentLim[d]))
        for d in range(len(dss_engine.ActiveCircuit.Transformers.AllNames)):
            self.influx_connector.set_time_step_data_point(esdl_id,
                                                          dss_engine.ActiveCircuit.Transformers.AllNames[d],
                                                          simulation_time, TransformerPower[d])
            self.influx_connector.set_time_step_data_point(esdl_id,
                                                          TransformerLimitNames[d],
                                                          simulation_time, TransformerPowerLim[d])
            LOGGER.debug('{0}: {1} MVA, limit={2} MVA'.format(dss_engine.ActiveCircuit.Transformers.AllNames[d], TransformerPower[d], TransformerPowerLim[d]))

        return ret_val
    
if __name__ == "__main__":
    helics_simulation_executor = CalculationServiceLVNetwork()
    helics_simulation_executor.start_simulation()
    helics_simulation_executor.stop_simulation()
