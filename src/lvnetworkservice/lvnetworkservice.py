# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
import time
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
    primary_trafo_busses : List[str]
    primary_voltage_bases : List[float]
    secondary_trafo_busses : List[str]
    secondary_voltage_bases: List[float]

@dataclass
class PowerFlowResult:
    bus_voltage_mag : List[float]
    total_line_current_mag : List[float]
    transformer_power : List[float]
    total_line_current_lim : List[float]
    transformer_power_lim : List[float]

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
                                    input_unit="VAr",
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
        self.transformer_section_start_marker = '! Trafo \n'
        self.load_definition_section_start_marker = '! Load Definitions \n'
        self.add_calculation(calculation_information)
        self.ems_list : dict[str, List[str]] = {}
        self.all_node_names : List[str] = []
        self.all_line_names : List[str] = []
        self.all_transformer_names : List[str] = []
        self.dss_file_name = "main.dss"

    def get_assets_of_type(self, assets : List[esdl.Asset], type):
        return [a for a in assets if isinstance(a, type)]

    def init_calculation_service(self, energy_system : esdl.EnergySystem):
        assets = energy_system.instance[0].area.asset
        self.network_name = energy_system.name.replace(" ", '_')
        lines_to_write = []
        dss_circuit_properties = self.build_base_dss_file(assets, lines_to_write)
        self.add_mv_network_to_main_dss(assets, lines_to_write)
        self.add_lv_networks_to_main_dss(assets, dss_circuit_properties)
        LOGGER.debug('OpenDSS compile network')
        self.dss_engine.Text.Command = f"compile {self.dss_file_name}"   
        self.all_node_names = self.dss_engine.ActiveCircuit.AllNodeNames
        self.all_line_names = self.dss_engine.ActiveCircuit.Lines.AllNames
        self.all_transformer_names = self.dss_engine.ActiveCircuit.Transformers.AllNames

    def generate_dss_electricity_cable(self, cable : esdl.ElectricityCable, bus_from : esdl.Joint, bus_to : esdl.Joint, include_ground = True):
        phases_specifications = '.1.2.3.4' if include_ground else '.1.2.3'
        phases = 4 if include_ground else 3
        dss_cable = 'New Line.' + cable.name + f' Phases={phases} Bus1=' + bus_from.name.split('Bus')[
                        0] + phases_specifications + ' Bus2=' + bus_to.name.split('Bus')[
                                     0] + phases_specifications + ' LineCode=' + cable.assetType + ' Length=' + str(
                        cable.length) + ' Units=m \n'
        return dss_cable

    def add_mv_network_to_main_dss(self, assets : List[esdl.Asset], lines_to_write : List[str]) -> DssCircuitProperties:

        self.add_mv_lines(assets, lines_to_write)

        with open(self.dss_file_name, "w") as f:
            f.writelines(lines_to_write)

        lines_to_write.clear()

        self.dss_engine.Text.Command = f"compile {self.dss_file_name}"

        self.cut_cable_in_mv_network(self.dss_file_name)
    
    def build_base_dss_file(self, assets : List[esdl.Asset], lines_to_write : List[str]) -> DssCircuitProperties:
        lines_to_write.append('Clear \n')
        lines_to_write.append('\nSet DefaultBaseFrequency=50 \n')

        self.generate_source(assets, lines_to_write)
        dss_circuit_properties = self.generate_trafos(assets, lines_to_write)

        return dss_circuit_properties

    def add_mv_lines(self, assets : List[esdl.Asset], lines_to_write : List[str]) -> DssCircuitProperties:
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


    def remove_cable_from_dss_file(self, joint_name1 : str, joint_name2 : str, file_name : str):
        with open(file_name, "r") as file:
            lines = file.readlines()

        line_sub_string_opt1 = f"Bus1={joint_name1} Bus2={joint_name2}"
        line_sub_string_opt2 = f"Bus1={joint_name2} Bus2={joint_name1}"
        found_line = next(line for line in lines if line_sub_string_opt1 in line or line_sub_string_opt2 in line)
        LOGGER.info(f"Removing line: {found_line}")
        lines.remove(found_line)

        with open(self.dss_file_name, "w") as file:
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
            self.dss_engine.ActiveCircuit.SetActiveElement(f"Line.{self.dss_engine.ActiveCircuit.Lines.AllNames[0]}")
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
        lines = []
        with open(Path(self.dss_file_name), "r") as f:
            lines = f.readlines()

        new_lines_descriptions = []

        self.add_lv_lines_to_network(assets, dss_circuit_properties, new_lines_descriptions)

        lines_index_start = lines.index(self.lines_section_start_marker)
        lines = lines[:lines_index_start+1] + new_lines_descriptions + lines[lines_index_start+1:]

        lines.append('\n')
        lines.append(self.load_definition_section_start_marker)
        self.add_loads_to_network(assets, lines)

        all_voltage_bases = set(dss_circuit_properties.primary_voltage_bases).union(set(dss_circuit_properties.secondary_voltage_bases))
        all_voltage_bases = sorted(all_voltage_bases, reverse=True)
        lines.append('\n! Final Configurations \n')
        lines.append(f"Set VoltageBases = {all_voltage_bases} \n") 
        lines.append("CalcVoltageBases \n") 
        for i, voltage_base in enumerate(dss_circuit_properties.primary_voltage_bases):
            lines.append(f'SetkVBase Bus={dss_circuit_properties.primary_trafo_busses[i]} kVLL={voltage_base}\n')
        for i, voltage_base in enumerate(dss_circuit_properties.secondary_voltage_bases):
            lines.append(f'SetkVBase Bus={dss_circuit_properties.secondary_trafo_busses[i]} kVLL={voltage_base}\n')

        # lines.append('CalcVoltageBases\n')

        lines.append('\n! Solve\n')

        lines.append('Set mode=snapshot\n')
        lines.append('! Solve\n')

        with open(self.dss_file_name, "w") as f:
            f.writelines(lines)

    def add_lv_lines_to_network(self, assets, dss_circuit_properties, new_lines_descriptions):
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

    def add_loads_to_network(self, assets, lines):
        for a in self.get_assets_of_type(assets, esdl.Building):
            name = ""
            e_connection = self.get_assets_of_type(a.asset, esdl.EConnection)[0]
            self.ems_list[e_connection.id] = []
            name = e_connection.name
            for electricity_demand in self.get_assets_of_type(a.asset, esdl.ElectricityDemand):
                # van 10 kv naar 0.4 kv basen
                lines.append(
                    'New Load.{name}_Ph1 Bus1={bus}.1.4 Phases=1 Conn=wye Model=1 kV=0.23 kW=1 kvar=0.0 \n'.format(
                        name=name, bus=name))
                lines.append(
                    'New Load.{name}_Ph2 Bus1={bus}.2.4 Phases=1 Conn=wye Model=1 kV=0.23 kW=1 kvar=0.0 \n'.format(
                        name=name, bus=name))
                lines.append(
                    'New Load.{name}_Ph3 Bus1={bus}.3.4 Phases=1 Conn=wye Model=1 kV=0.23 kW=1 kvar=0.0 \n'.format(
                        name=name, bus=name))
                self.ems_list[e_connection.id].append(f"Load.{name}_Ph1")
                self.ems_list[e_connection.id].append(f"Load.{name}_Ph2")
                self.ems_list[e_connection.id].append(f"Load.{name}_Ph3")

    def generate_trafos(self, assets : List[esdl.Asset], lines_to_write : List[str]) -> DssCircuitProperties:
        lines_to_write.append('\n! Trafo XFMRCodes \n')
        lines_to_write.append('Redirect XFMRCode.dss \n')
        lines_to_write.append('\n')
        lines_to_write.append(self.transformer_section_start_marker)
        dss_circuit_properties = DssCircuitProperties([], [], [], [])

        for a in self.get_assets_of_type(assets, esdl.Transformer):
            for port in a.port:
                if isinstance(port, esdl.InPort):
                    busFrom = port.connectedTo[0].energyasset
                    dss_circuit_properties.primary_voltage_bases.append(a.voltagePrimary)
                    dss_circuit_properties.primary_trafo_busses.append(busFrom.name.split('Bus')[0])
                else:
                    busTo = port.connectedTo[0].energyasset
                    dss_circuit_properties.secondary_voltage_bases.append(a.voltageSecundary)
                    dss_circuit_properties.secondary_trafo_busses.append(busTo.name.split('Bus')[0])
            lines_to_write.append(
                'New Transformer.{name} Xfmrcode={type} Buses=[{bus1}  {bus2}.1.2.3] kVs=[{Uprim} {Usecund}] \n'.format(
                    name=a.name, type=a.assetType, bus1=busFrom.name.split('Bus')[0],
                    bus2=busTo.name.split('Bus')[0], Uprim=a.voltagePrimary, Usecund=a.voltageSecundary))
        return dss_circuit_properties

    def generate_source(self, assets : List[esdl.Asset], lines_to_write : List[str]) -> DssCircuitProperties:
        LOGGER.debug(self.network_name)
        lines_to_write.append('\n! Swing or Source Bar \n')

        import_count = 0

        for a in self.get_assets_of_type(assets, esdl.Import):
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


    def load_flow_current_step(self, param_dict : dict, simulation_time : datetime, time_step_number : TimeStepInformation, esdl_id : EsdlId, energy_system : EnergySystem):
        
        self.set_load_flow_parameters(param_dict)

        self.do_load_flow()

        start = time.time()
        results = self.process_results()
        end = time.time()
        LOGGER.info(f"Processing results took {end - start} seconds")
        start = time.time()
        self.write_results_to_influx(esdl_id, simulation_time, results)
        end = time.time()
        LOGGER.info(f"Writing results took {end - start} seconds")

        return {}


    def set_load_flow_parameters(self, param_dict : dict):
        # START user calc
        LOGGER.info("calculation 'load_flow_current_step' started")     

        LOGGER.debug('OpenDSS add loads to network')

        self.dss_engine.ActiveCircuit.SetActiveElement(f"{self.ems_list[list(self.ems_list.keys())[0]][0]}")
        property_mapping : dict [str, int] = {
            "kW" : 0,
            "kvar" : 0
        }
        for i, prop_name in enumerate(self.dss_engine.ActiveCircuit.ActiveCktElement.AllPropertyNames):
            if prop_name in property_mapping:
                property_mapping[prop_name] = i
        for id in self.ems_list:
            num_phases = len(param_dict[f'EConnection/aggregated_active_power/{id}'])
            for i, name in enumerate(self.ems_list[id]):
                if i < num_phases:
                    self.dss_engine.ActiveCircuit.SetActiveElement(name)
                    active_ckt_element = self.dss_engine.ActiveCircuit.ActiveCktElement
                    active_load = param_dict[f'EConnection/aggregated_active_power/{id}'][i] * 1e-3
                    reactive_load = param_dict[f'EConnection/aggregated_reactive_power/{id}'][i] * 1e-3
                    if active_ckt_element.AllPropertyNames[property_mapping["kW"]] != "kW" or active_ckt_element.AllPropertyNames[property_mapping["kvar"]] != "kvar":
                        raise ValueError("Property mapping for kW or kvar is incorrect")
                    active_ckt_element.Properties[property_mapping["kW"]].Val = active_load
                    active_ckt_element.Properties[property_mapping["kvar"]].Val = reactive_load


    def do_load_flow(self):
        LOGGER.debug('OpenDSS solve loadflow calculation')
        self.dss_engine.ActiveCircuit.Solution.Solve()

    def process_results(self) -> PowerFlowResult:
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
        for name in self.all_line_names:
            self.dss_engine.ActiveCircuit.SetActiveElement(
                'Line.{0}'.format(name))
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

        return PowerFlowResult(BusVoltageMag, TotalLineCurrentMag, TransformerPower, TotalLineCurrentLim, TransformerPowerLim)

    def write_results_to_influx(self, esdl_id : EsdlId, simulation_time : datetime, power_flow_result : PowerFlowResult):
        # Write results to influxdb
        amount_of_node_values = len(power_flow_result.bus_voltage_mag)
        amount_of_line_values = len(power_flow_result.total_line_current_mag) + len(power_flow_result.total_line_current_lim)
        amount_of_transformer_values = len(power_flow_result.transformer_power) + len(power_flow_result.transformer_power_lim)
        LOGGER.debug(f'Writing {amount_of_node_values} node values to influxdb')
        LOGGER.debug(f'Writing {amount_of_line_values} line values to influxdb')
        LOGGER.debug(f'Writing {amount_of_transformer_values} transformer values to influxdb')
        LOGGER.debug(f'Writing a total of {sum([amount_of_line_values, amount_of_transformer_values, amount_of_node_values])} values to influxdb')
        line_limit_names = [x + '_limit' for x in self.all_line_names]
        for d in range(len(self.all_node_names)):
            voltage_value = power_flow_result.bus_voltage_mag[d]
            self.influx_connector.set_time_step_data_point(esdl_id, self.all_node_names[d],
                                                      simulation_time, voltage_value)
        for d in range(len(self.all_line_names)):
            self.influx_connector.set_time_step_data_point(esdl_id, self.all_line_names[d],
                                                          simulation_time, power_flow_result.total_line_current_mag[d])
            self.influx_connector.set_time_step_data_point(esdl_id, line_limit_names[d], simulation_time,
                                                          power_flow_result.total_line_current_lim[d])
        for d in range(len(self.dss_engine.ActiveCircuit.Transformers.AllNames)):
            name = self.dss_engine.ActiveCircuit.Transformers.AllNames[d]
            self.influx_connector.set_time_step_data_point(esdl_id,
                                                          self.dss_engine.ActiveCircuit.Transformers.AllNames[d],
                                                          simulation_time, power_flow_result.transformer_power[d])
            self.influx_connector.set_time_step_data_point(esdl_id, f"{name}_limit", simulation_time,
                                                          power_flow_result.transformer_power_lim[d])

if __name__ == "__main__":
    helics_simulation_executor = CalculationServiceLVNetwork()
    helics_simulation_executor.start_simulation()
    helics_simulation_executor.stop_simulation()
