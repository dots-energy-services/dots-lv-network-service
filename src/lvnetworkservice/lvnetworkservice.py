# -*- coding: utf-8 -*-
from datetime import datetime
from io import TextIOWrapper
from typing import List
from esdl import esdl
import helics as h
from dots_infrastructure.DataClasses import EsdlId, HelicsCalculationInformation, SubscriptionDescription, TimeStepInformation
from dots_infrastructure.HelicsFederateHelpers import HelicsSimulationExecutor
from dots_infrastructure.Logger import LOGGER
from esdl import EnergySystem
import networkx as nx
import dss
import math
from dataclasses import dataclass

@dataclass
class DssCircuitProperties:
    secondary_trafo_busses : List[str]
    voltage_bases: List[float]

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
        self.dss_engine = dss.DSS
        self.lines_section_start_marker = '! Lines \n'
        self.add_calculation(calculation_information)

    def get_assets_of_type(self, assets : List[esdl.Asset], type):
        return [a for a in assets if isinstance(a, type)]

    def init_calculation_service(self, energy_system : esdl.EnergySystem):
        assets = energy_system.instance[0].area.asset
        self.network_name = energy_system.name.replace(" ", '_')
        dss_circuit_properties = self.generate_mv_dss(assets)
        self.add_lv_networks_to_main_dss(assets, dss_circuit_properties)

    def generate_dss_electricity_cable(self, cable : esdl.ElectricityCable, bus_from : esdl.Joint, bus_to : esdl.Joint, include_ground = True):
        phases_specifications = '.1.2.3.4' if include_ground else '.1.2.3'
        phases = 4 if include_ground else 3
        dss_cable = 'New Line.' + cable.name + f' Phases={phases} Bus1=' + bus_from.name.split('Bus')[
                        0] + phases_specifications + ' Bus2=' + bus_to.name.split('Bus')[
                                     0] + phases_specifications + ' LineCode=' + cable.assetType + ' Length=' + str(
                        cable.length) + ' Units=m \n'
        return dss_cable

    def generate_mv_dss(self, assets : List[esdl.Asset]) -> DssCircuitProperties:
        file_name = "main.dss"
        dss_circuit_properties = self.build_mv_network_dss_file(assets, file_name)

        self.dss_engine.Text.Command = "compile main.dss"

        self.cut_cable_in_mv_network(file_name)

        return dss_circuit_properties

    def build_mv_network_dss_file(self, assets, file_name) -> DssCircuitProperties:
        lines_to_write = []
        lines_to_write.append('Clear \n')
        lines_to_write.append('\nSet DefaultBaseFrequency=50 \n')

        voltage_bases = self.generate_source(assets, lines_to_write)
        secondary_trafo_busses = self.generate_trafos(assets, lines_to_write, voltage_bases)

        lines_to_write.append('\n! LineCodes \n')
        lines_to_write.append('Redirect LineCode.dss \n')
        lines_to_write.append('\n')
        lines_to_write.append(self.lines_section_start_marker)

        for a in self.get_assets_of_type(assets, esdl.ElectricityCable):
            if "mv_cable" in a.name.lower():
                for port in a.port:
                    if isinstance(port, esdl.InPort):
                        bus_from = port.connectedTo[0].energyasset
                    else:
                        bus_to = port.connectedTo[0].energyasset
                dss_cable = self.generate_dss_electricity_cable(a, bus_from, bus_to, False)
                lines_to_write.append(dss_cable)
        
        with open(file_name, "w") as file:
            file.writelines(lines_to_write)
        lines_to_write.clear()
        return DssCircuitProperties(secondary_trafo_busses, voltage_bases)

    def remove_cable_from_dss_file(self, joint_name1 : str, joint_name2 : str, file_name : str):
        with open(file_name, "r") as file:
            lines = file.readlines()

        line_sub_string_opt1 = f"Bus1={joint_name1} Bus2={joint_name2}"
        line_sub_string_opt2 = f"Bus1={joint_name2} Bus2={joint_name1}"
        found_line = next(line for line in lines if line_sub_string_opt1 in line or line_sub_string_opt2 in line)
        LOGGER.info(f"Removing line: {found_line}")
        lines.remove(found_line)

        with open("main.dss", "w") as file:
            file.writelines(lines)

    def cut_cable_in_mv_network(self, file_name : str):
        graph = self.build_mv_network_graph()

        if len(graph.nodes) > 0:
            source_bus = "jointhighvoltagetrafo.1.2.3"

            impedance_distances = nx.single_source_dijkstra_path_length(graph, source_bus, weight="weight")
            joint_max_impedence_distance = max(impedance_distances, key = impedance_distances.get)
    
            edges_max_distance = graph.edges([joint_max_impedence_distance])
            max_impedence_distance = 0
            to_node_with_max_distance = None
            for edge in edges_max_distance:
                to_node = edge[1] if edge[0] == joint_max_impedence_distance else edge[0]
                impedence_distance = impedance_distances[to_node]
                if impedence_distance > max_impedence_distance:
                    max_impedence_distance = impedence_distance
                    to_node_with_max_distance = to_node
            cable_to_remove = (joint_max_impedence_distance, to_node_with_max_distance)
            self.remove_cable_from_dss_file(cable_to_remove[0], cable_to_remove[1], file_name)

    def build_mv_network_graph(self) -> nx.Graph:
        graph = nx.Graph()
        if self.dss_engine.ActiveCircuit.Lines.AllNames != ['NONE']:
            self.dss_engine.ActiveCircuit.SetActiveElement(self.dss_engine.ActiveCircuit.Lines.AllNames[0])
            property_mapping = {
                "Bus1" : "",
                "Bus2" : "",
                "R1" : 0,
                "X1" : 0,
                "Length" : 0
            }
            for i, prop_name in enumerate(self.dss_engine.ActiveCircuit.ActiveCktElement.AllPropertyNames):
                if prop_name in property_mapping:
                    property_mapping[prop_name] = i

            for l in range(len(self.dss_engine.ActiveCircuit.Lines.AllNames)):
                LOGGER.info(f"Setting active element: {'Line.{0}'.format(self.dss_engine.ActiveCircuit.Lines.AllNames[l])}")
    
                self.dss_engine.ActiveCircuit.SetActiveElement(
                    'Line.{0}'.format(self.dss_engine.ActiveCircuit.Lines.AllNames[l]))
                active_ckt_element = self.dss_engine.ActiveCircuit.ActiveCktElement
    
                property_values = {}
                for key, index in property_mapping.items():
                    property_values[key] = active_ckt_element.Properties[index].Val
                bus1 = active_ckt_element.BusNames[0]
                bus2 = active_ckt_element.BusNames[1]
                LOGGER.info(f"Processing line between bus {bus1} and bus {bus2} with length {property_values["Length"]}")
                length = float(property_values["Length"])
                r1 = float(property_values["R1"]) * length
                x1 = float(property_values["X1"]) * length
                LOGGER.info(f"Line R1 value: {r1}, Line X1 value {x1}")
                impedance = (r1**2 + x1**2)**0.5
                graph.add_edge(bus1, bus2, weight=impedance)
                LOGGER.info(f"Added edge {bus1} - {bus2} with impedance {impedance}")
    
            assert len(graph.edges) == len(self.dss_engine.ActiveCircuit.Lines.AllNames)
        return graph

    def add_lv_networks_to_main_dss(self, assets : List[esdl.Asset], dss_circuit_properties : DssCircuitProperties):
        self.ems_list = []
        lines = []
        with open("main.dss", "r") as f:
            lines = f.readlines()

        new_lines_descriptions = []

        for a in self.get_assets_of_type(assets, esdl.ElectricityCable):
            if "mv_cable" not in a.name.lower():
                for port in a.port:
                    if isinstance(port, esdl.InPort):
                        busFrom = port.connectedTo[0].energyasset
                    else:
                        busTo = port.connectedTo[0].energyasset
                length = a.length
                linecode = a.assetType
                if busFrom.name.split('Bus')[0] in dss_circuit_properties.secondary_trafo_busses:
                    new_lines_descriptions.append('New Line.' + a.name + ' Phases=4 Bus1=' + busFrom.name.split('Bus')[
                        0] + '.1.2.3.0' + ' Bus2=' + busTo.name.split('Bus')[
                                     0] + '.1.2.3.4 LineCode=' + linecode + ' Length=' + str(
                        length) + ' Units=m \n')
                else:
                    new_lines_descriptions.append('New Line.' + a.name + ' Phases=4 Bus1=' + busFrom.name.split('Bus')[
                        0] + '.1.2.3.4' + ' Bus2=' + busTo.name.split('Bus')[
                                     0] + '.1.2.3.4 LineCode=' + linecode + ' Length=' + str(
                        length) + ' Units=m \n')

        lines_index_start = lines.index(self.lines_section_start_marker)
        lines = lines[:lines_index_start+1] + new_lines_descriptions + lines[lines_index_start+1:]

        lines.append('\n! Load Definitions \n')
        for a in self.get_assets_of_type(assets, esdl.Building):
            name = ""
            e_connection = self.get_assets_of_type(a.asset, esdl.EConnection)[0]
            self.ems_list.append(e_connection.id)
            name = e_connection.name
            for electricity_demand in self.get_assets_of_type(a.asset, esdl.ElectricityDemand):
                # van 10 kv naar 0.4 kv basen
                lines.append(
                    'New Load.{name}_Ph1 Bus1={bus}.1.4 Phases=1 Conn=wye Model=1 kV=0.230 kW=1 PF=1.0 Vmaxpu=1.5 Vminpu=0.60 \n'.format(
                        name=name, bus=name))
                lines.append(
                    'New Load.{name}_Ph2 Bus1={bus}.2.4 Phases=1 Conn=wye Model=1 kV=0.230 kW=1 PF=1.0 Vmaxpu=1.5 Vminpu=0.60 \n'.format(
                        name=name, bus=name))
                lines.append(
                    'New Load.{name}_Ph3 Bus1={bus}.3.4 Phases=1 Conn=wye Model=1 kV=0.230 kW=1 PF=1.0 Vmaxpu=1.5 Vminpu=0.60 \n'.format(
                        name=name, bus=name))


        lines.append('\n! Final Configurations \n')
        lines.append('Set VoltageBases = {0}\n'.format(dss_circuit_properties.voltage_bases))
        lines.append('CalcVoltageBases\n')

        lines.append('\n! Solve\n')

        lines.append('Set mode=snapshot\n')
        lines.append('! Solve\n')

        with open("main.dss", "w") as f:
            f.writelines(lines)

    def generate_trafos(self, assets : List[esdl.Asset], lines_to_write : List[str], voltage_bases : List[float]) -> List[str]:
        lines_to_write.append('\n! Trafo XFMRCodes \n')
        lines_to_write.append('Redirect XFMRCode.dss \n')
        lines_to_write.append('\n! Trafo \n')

        secondary_trafo_bus = []
        for a in self.get_assets_of_type(assets, esdl.Transformer):
            voltage_bases.append(a.voltageSecundary)
            for port in a.port:
                if isinstance(port, esdl.InPort):
                    busFrom = port.connectedTo[0].energyasset
                else:
                    busTo = port.connectedTo[0].energyasset
                    secondary_trafo_bus.append(busTo.name.split('Bus')[0])
            lines_to_write.append(
                'New Transformer.{name} Xfmrcode={type} Buses=[{bus1}  {bus2}.1.2.3] kVs=[{Uprim} {Usecund}] \n'.format(
                    name=a.name, type=a.assetType, bus1=busFrom.name.split('Bus')[0],
                    bus2=busTo.name.split('Bus')[0], Uprim=a.voltagePrimary, Usecund=a.voltageSecundary))
        return secondary_trafo_bus

    def generate_source(self, assets : List[esdl.Asset], lines_to_write : List[str]) -> List[float]:
        LOGGER.debug(self.network_name)
        voltage_bases = []
        lines_to_write.append('\n! Swing or Source Bar \n')

        import_count = 0

        for a in self.get_assets_of_type(assets, esdl.Import):
            voltage_bases.append(float(a.assetType))
            import_count += 1
            for port in a.port:
                busTo = port.connectedTo[0].energyasset
            if import_count == 1:
                lines_to_write.append(
                    'New circuit.{network} phases=3 pu=1.0 basekv={Uref} bus1={bus1} \n'.format(
                        network='{0}_{1}'.format(self.network_name,import_count),
                        Uref=a.assetType, bus1=
                        busTo.name.split('Bus')[
                            0]))
            else:
                lines_to_write.append(
                    'New Vsource.{network} phases=3 pu=1.0 basekv={Uref} bus1={bus1} \n'.format(
                        network='{0}_{1}'.format(self.network_name, import_count),
                        Uref=a.assetType, bus1=
                        busTo.name.split('Bus')[
                            0]))

        return voltage_bases

    def load_flow_current_step(self, param_dict : dict, simulation_time : datetime, time_step_number : TimeStepInformation, esdl_id : EsdlId, energy_system : EnergySystem):
        # START user calc
        LOGGER.info("calculation 'load_flow_current_step' started")

        # ------------------
        # OpenDSS simulation
        # ------------------
        # Define and compile network
        LOGGER.debug('OpenDSS compile network')
        self.dss_engine.Text.Command = f"compile Main.dss"

        totalactiveload = 0
        totalreactiveload = 0
        # Receive load values from EMS and adjust load values:

        LOGGER.debug('OpenDSS add loads to network')
        connection = 0

        self.dss_engine.ActiveCircuit.Loads.First
        while connection < len(self.ems_list):
            # Determine the number of phases for the current connection
            num_phases = len(param_dict['EConnection/aggregated_active_power/{0}'.format(self.ems_list[connection])])
            # Loop through phases for the current connection
            phase = 0
            while phase < num_phases:
                param_key = 'EConnection/aggregated_active_power/{0}'.format(self.ems_list[connection])
                self.dss_engine.ActiveCircuit.Loads.kW = param_dict[param_key][phase] * 1e-3
                totalactiveload += param_dict['EConnection/aggregated_active_power/{0}'.format(self.ems_list[connection])][phase] * 1e-3
                LOGGER.debug('Active power {0} {1}'.format(self.dss_engine.ActiveCircuit.Loads.Name,
                      self.dss_engine.ActiveCircuit.Loads.kW))

                self.dss_engine.ActiveCircuit.Loads.kvar = param_dict['EConnection/aggregated_reactive_power/{0}'.format(self.ems_list[connection])][phase] * 1e-3
                totalreactiveload += param_dict['EConnection/aggregated_reactive_power/{0}'.format(self.ems_list[connection])][phase] * 1e-3
                LOGGER.debug('Reactive power {0} {1}'.format(self.dss_engine.ActiveCircuit.Loads.Name,
                             self.dss_engine.ActiveCircuit.Loads.kva))

                self.dss_engine.ActiveCircuit.Loads.Next
                phase += 1
            connection += 1

        # Solve load flow calculation
        LOGGER.debug('OpenDSS solve loadflow calculation')
        self.dss_engine.ActiveCircuit.Solution.Solve()

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
        for i in range(len(self.dss_engine.ActiveCircuit.AllBusVmag)):
            BusVoltageMag.append(round(self.dss_engine.ActiveCircuit.AllBusVmag[i], 2))

        # Phase current magnitudes and angles for each line:
        LOGGER.debug('Extract current magnitudes and angles')
        for l in range(self.dss_engine.ActiveCircuit.Lines.Count):
            self.dss_engine.ActiveCircuit.SetActiveElement(
                'Line.{0}'.format(self.dss_engine.ActiveCircuit.Lines.AllNames[l]))
            Total_line_current = 0
            for i in range(1, 4):
                LineCurrentMag.append(
                    round(self.dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2], 2))
                LineCurrentAng.append(
                    round(self.dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2 + 1], 2))
                Total_line_current += round(self.dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2],
                                            2)
            TotalLineCurrentMag.append(Total_line_current)
            TotalLineCurrentLim.append(float(self.dss_engine.ActiveCircuit.ActiveCktElement.NormalAmps))

        # Apparent power for each transformer:
        LOGGER.debug('Extract apparent power for each transformer')
        self.dss_engine.ActiveCircuit.Transformers.First
        for t in range(self.dss_engine.ActiveCircuit.Transformers.Count):
            self.dss_engine.ActiveCircuit.SetActiveElement(
                'Transformer.{0}'.format(self.dss_engine.ActiveCircuit.Transformers.AllNames[t]))
            TransformerPower.append(round(math.sqrt(self.dss_engine.ActiveCircuit.ActiveElement.TotalPowers[0] ** 2 +
                                                    self.dss_engine.ActiveCircuit.ActiveElement.TotalPowers[1] ** 2), 2))

            TransformerPowerLim.append((self.dss_engine.ActiveCircuit.Transformers.kVA))
            self.dss_engine.ActiveCircuit.Transformers.Next
        LineLimitNames = [x + '_limit' for x in self.dss_engine.ActiveCircuit.Lines.AllNames]
        TransformerLimitNames = [x + '_limit' for x in self.dss_engine.ActiveCircuit.Transformers.AllNames]

        ret_val = {}

        # remove all the limits

        # Write results to influxdb
        amount_of_node_values = len(self.dss_engine.ActiveCircuit.AllNodeNames)
        amount_of_line_values = len(self.dss_engine.ActiveCircuit.Lines.AllNames)
        amount_of_transformer_values = len(self.dss_engine.ActiveCircuit.Transformers.AllNames)
        LOGGER.info(f'Writing {amount_of_node_values} node values to influxdb')
        LOGGER.info(f'Writing {amount_of_line_values} line values to influxdb')
        LOGGER.info(f'Writing {amount_of_transformer_values} transformer values to influxdb')
        LOGGER.info(f'Writing a total of {sum([amount_of_line_values, amount_of_transformer_values])} values to influxdb')
        # for d in range(len(self.dss_engine.ActiveCircuit.AllNodeNames)):
        #     self.influx_connector.set_time_step_data_point(esdl_id, dss_engine.ActiveCircuit.AllNodeNames[d],
        #                                                   simulation_time, BusVoltageMag[d])
        #     LOGGER.debug('{0}: {1} V'.format(dss_engine.ActiveCircuit.AllNodeNames[d], BusVoltageMag[d]))
        for d in range(len(self.dss_engine.ActiveCircuit.Lines.AllNames)):
            self.influx_connector.set_time_step_data_point(esdl_id, self.dss_engine.ActiveCircuit.Lines.AllNames[d],
                                                          simulation_time, TotalLineCurrentMag[d])
            # self.influx_connector.set_time_step_data_point(esdl_id, LineLimitNames[d], simulation_time,
            #                                               TotalLineCurrentLim[d])
            LOGGER.debug('{0}: {1} A, limit={2} A'.format(self.dss_engine.ActiveCircuit.Lines.AllNames[d], TotalLineCurrentMag[d], TotalLineCurrentLim[d]))
        for d in range(len(self.dss_engine.ActiveCircuit.Transformers.AllNames)):
            self.influx_connector.set_time_step_data_point(esdl_id,
                                                          self.dss_engine.ActiveCircuit.Transformers.AllNames[d],
                                                          simulation_time, TransformerPower[d])
            # self.influx_connector.set_time_step_data_point(esdl_id,
            #                                               TransformerLimitNames[d],
            #                                               simulation_time, TransformerPowerLim[d])
            LOGGER.debug('{0}: {1} MVA, limit={2} MVA'.format(self.dss_engine.ActiveCircuit.Transformers.AllNames[d], TransformerPower[d], TransformerPowerLim[d]))

        return ret_val
    
if __name__ == "__main__":
    helics_simulation_executor = CalculationServiceLVNetwork()
    helics_simulation_executor.start_simulation()
    helics_simulation_executor.stop_simulation()
