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
import dss          #may be removed eventually
import math
from dataclasses import dataclass
from power_grid_model.utils import json_serialize_to_file

import numpy as np
import pandas as pd

from power_grid_model import (
    CalculationMethod,
    CalculationType,
    ComponentAttributeFilterOptions,
    ComponentType,
    DatasetType,
    LoadGenType,
    PowerGridModel,
    attribute_dtype,
    initialize_array,
)
from power_grid_model.validation import assert_valid_input_data


@dataclass
class DssCircuitProperties:
    primary_trafo_busses : List[str]
    primary_voltage_bases : List[float]
    secondary_trafo_busses : List[str]
    secondary_voltage_bases: List[float]

@dataclass
class OGMNetworkProperties:
    pass


@dataclass
class PowerFlowResult:
     bus_voltage_mag : List[float]
     bus_voltage_ang : List[float]
    # total_line_current_mag_from : List[float]
    # total_line_current_mag_to : List[float]
    # line_current_ang_from : List[float]
    # line_current_ang_to : List[float]
    # transformer_power_from : List[float]
    # transformer_power_to : List[float]
    # total_line_current_lim : List[float]
    # transformer_power_lim : List[float]

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
        self.output_frame = pd.DataFrame()
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
        self.network_name = energy_system.name.replace(" ", '_') #stay the same for OGM?
        
       

         
        #self.add_mv_network_to_main_dss(assets, lines_to_write) #Adds MV network to dss file (prob start here)
        
        self.add_lv_networks_to_main_OGM(assets) #Adds LV network to OGM
        



    # def add_mv_network_to_main_ogm(self, assets: List[esdl.Asset]) -> OGMNetworkProperties:
    #     #pass #this only says this function exists but nothing in it
    #     joints = [a for a in esdl.EnergySystem.eAllContents() if isinstance(a, esdl.Joint)]
    #     node = initialize_array(DatasetType.input, ComponentType.node, len(joints)) #Instead of 3, there should be the number of nodes
    #     node["id"] = np.array(joints.id)
        
    #     #mv_cables = [a for a in assets if isinstance(a, esdl.ElectricityCable) and "vmv" in a.assetType.lower()]
    #     line = initialize_array(DatasetType.input, ComponentType.line, len(mv_cables))
    #     line["id"] = np.array(mv_cables.id)
    #     #line["from_node"] = np.array(mv_cables.) #from out_port Continue here
    #     LOGGER.info("Number of Nodes: len(joints)")

    def add_mv_network_to_main_dss(self, assets : List[esdl.Asset], lines_to_write : List[str]) -> DssCircuitProperties:

        self.add_mv_lines(assets, lines_to_write)

        with open(self.dss_file_name, "w") as f:   #Is not necessary for OGM I think
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



    def normalize_id(self, ref):
        if ref is None:
            return None
        if hasattr(ref, "id"):
            return str(ref.id)
        if hasattr(ref, "__iter__") and not isinstance(ref, str):
            for item in ref:
                if hasattr(item, "id"):
                    return str(item.id)
        return str(ref)

    def find_joint_id_by_port(self, port_id: str) -> int | None:
        for j in self.joints:
            for p in j.port:
                if p.id == port_id:
                    return self.joint_id_map[j.id]
        return 
    
    def econ_port_to_econ_id(self, port_id: str) -> int | None:
        for j in self.econnections:
            for p in j.port:
               if p.id == port_id:
                    return port_id
        return None 
    
  

    def add_lv_networks_to_main_OGM(self, assets: List[esdl.Asset]) -> OGMNetworkProperties:
        self.source = [a for a in assets if isinstance(a, esdl.Import)]
        self.transformer = [a for a in assets if isinstance(a, esdl.Transformer)]
        self.Buildings = [a for a in assets if isinstance(a, esdl.Building)]
        self.econnections = []
        for building in self.Buildings:
            for asset in building.asset:
                if isinstance(asset, esdl.EConnection):
                    self.econnections.append(asset)
        
        self.joints = [a for a in assets if isinstance(a, esdl.Joint)]
        self.joint_id_map = {j.id: i + 1 for i, j in enumerate(self.joints)}

        lv_cables = [a for a in assets if isinstance(a, esdl.ElectricityCable)] #esdl.ElectricityCable does not see the difference between LV and MV cable. Look into later
        cable_id_map = {c.id: i + 1 for i, c in enumerate(lv_cables)}   # Also takes the HomeCables into account. -> 11 cables in test

        from_nodes = []
        to_nodes = []
        Econ_node = []
        cable_lengths_map = []
        for c in lv_cables:
            cable_lengths_map.append(c.length)
            from_port = next((p for p in c.port if isinstance(p, esdl.InPort)), None)
            to_port = next((p for p in c.port if isinstance(p, esdl.OutPort)), None)

            from_id = self.normalize_id(getattr(from_port, "connectedTo", None))
            to_id = self.normalize_id(getattr(to_port, "connectedTo", None))
            
            from_joint_id = self.find_joint_id_by_port(from_id) if from_port else None
            to_joint_id = self.find_joint_id_by_port(to_id) if to_port else None

            if to_joint_id is None:
                econ_id = self.econ_port_to_econ_id(to_id)
                to_joint_id = len(self.joint_id_map) + 1
                Econ_node.append(to_joint_id)
                self.joint_id_map[econ_id] = to_joint_id
                #Do something if econ_id = None

            from_nodes.append(from_joint_id)
            to_nodes.append(to_joint_id)

        self.asym_line_from_nodes = np.array(from_nodes) #Used in processing results

        source_to_node = []
        for source in self.source:
            out_port = next((p for p in source.port if isinstance(p, esdl.OutPort)), None)
            if out_port is None:
                continue  # skip if no OutPort
    
            connected_uuid = self.normalize_id(getattr(out_port, "connectedTo", None))
            for joint in self.joints:
                inport = next((p for p in joint.port if isinstance(p, esdl.InPort)), None)
                if inport.id == connected_uuid:
                    source_to_node_id = self.joint_id_map[joint.id]
                    source_to_node.append(source_to_node_id)
                    

        print("from_nodes:", from_nodes)
        print("to_nodes:", to_nodes)
        node_to_trafo = []
        trafo_to_node = []
        primary_voltage = []
        secundary_voltage = []
        node_voltage_map = {}
        for trafo in self.transformer:
            primary_voltage.append(trafo.voltagePrimary)
            secundary_voltage.append(trafo.voltageSecundary)    
            from_joint_to_trafo = next((p for p in trafo.port if isinstance(p, esdl.InPort)), None)
            from_trafo_to_joint = next((p for p in trafo.port if isinstance(p, esdl.OutPort)), None)
            connected_uuid_in = self.normalize_id(getattr(from_joint_to_trafo, "connectedTo", None))
            connected_uuid_out = self.normalize_id(getattr(from_trafo_to_joint, "connectedTo", None))
            for joint in self.joints:
                inport = next((p for p in joint.port if isinstance(p, esdl.InPort)), None)
                outport = next((p for p in joint.port if isinstance(p, esdl.OutPort)), None)
                if inport and inport.id == connected_uuid_out:
                    trafo_to_node_id = self.joint_id_map[joint.id]
                    trafo_to_node.append(trafo_to_node_id)
                    node_voltage_map[trafo_to_node_id] = trafo.voltageSecundary
                    
                

                if outport and outport.id == connected_uuid_in:
                    node_to_trafo_id = self.joint_id_map[joint.id]
                    node_to_trafo.append(node_to_trafo_id)
                    node_voltage_map[node_to_trafo_id] = trafo.voltagePrimary
                 
        #Initialize node array
        DEFAULT_LV_VOLTAGE = 0.4 #als backup voor nodes zonder trafo
        for node_id in self.joint_id_map.values():
            if node_id not in node_voltage_map:
                node_voltage_map[node_id] = DEFAULT_LV_VOLTAGE

        node_ids = np.array(list(self.joint_id_map.values()))
        self.node = initialize_array(DatasetType.input, ComponentType.node, len(node_ids), empty=True) #Load toevoegen voor created nodes 
        self.node["id"] = node_ids
        self.node["u_rated"] = np.array([node_voltage_map[nid] for nid in node_ids])*1000 #in V

        ID_counter = len(node_ids) + 1

        #Initialize line array
        cable_length_kms = np.array(cable_lengths_map) / 1000  # Convert length to kilometers
        n = len(lv_cables)
        self.asym_line = initialize_array(DatasetType.input,ComponentType.asym_line, n)

        self.asym_line["id"] = np.array([i + ID_counter for i in range(n)])
        self.asym_line["from_node"] = from_nodes
        self.asym_line["to_node"] = to_nodes
        self.asym_line["from_status"] = np.ones(n)
        self.asym_line["to_status"] = np.ones(n)

        # Resistance matrix (Ohm )

        self.asym_line["r_aa"] = np.full(n, 0.17347001) * cable_length_kms
        self.asym_line["r_ba"] = np.full(n, 0.04947) * cable_length_kms
        self.asym_line["r_ca"] = np.full(n, 0.04947) * cable_length_kms

        self.asym_line["r_bb"] = np.full(n, 0.17347001) * cable_length_kms
        self.asym_line["r_cb"] = np.full(n, 0.04947) * cable_length_kms

        self.asym_line["r_cc"] = np.full(n, 0.17347001) * cable_length_kms
        self.asym_line["r_na"] = np.full(n, 0.04947) * cable_length_kms
        self.asym_line["r_nb"] = np.full(n, 0.04947) * cable_length_kms
        self.asym_line["r_nc"] = np.full(n, 0.04947) * cable_length_kms
        self.asym_line["r_nn"] = np.full(n, 0.17347001) * cable_length_kms



        # Reactance matrix (Ohm )

        self.asym_line["x_aa"] = np.full(n, 0.81673998) * cable_length_kms
        self.asym_line["x_ba"] = np.full(n, 0.74563998) * cable_length_kms
        self.asym_line["x_bb"] = np.full(n, 0.81673998) * cable_length_kms
        self.asym_line["x_ca"] = np.full(n, 0.72983998) * cable_length_kms

        self.asym_line["x_cb"] = np.full(n, 0.74563998) * cable_length_kms

        self.asym_line["x_cc"] = np.full(n, 0.81673998) * cable_length_kms
        self.asym_line["x_na"] = np.full(n, 0.74563998) * cable_length_kms
        self.asym_line["x_nb"] = np.full(n, 0.72983998) * cable_length_kms
        self.asym_line["x_nc"] = np.full(n, 0.74563998) * cable_length_kms
        self.asym_line["x_nn"] = np.full(n, 0.81673998) * cable_length_kms

        self.asym_line["i_n"] = np.full(n, 414.0)
        self.asym_line["c0"] = np.full(len(lv_cables), 0.43200001e-9)
        self.asym_line["c1"] = np.full(len(lv_cables), 0.7200000e-9)


        # Capacitance (F )

        # asym_line["c_aa"] = np.full(n, 0.72000003e-9) * cable_length_kms
        # asym_line["c_ba"] = np.full(n, 0.72000003e-9) * cable_length_kms
        # asym_line["c_ca"] = np.full(n, 0.72000003e-9) * cable_length_kms

        # asym_line["c_bb"] = np.full(n, 0.72000003e-9) * cable_length_kms
        # asym_line["c_cb"] = np.full(n, 0.72000003e-9) * cable_length_kms

        # asym_line["c_cc"] = np.full(n, 0.72000003e-9) * cable_length_kms


        # asym_line["r0"] = np.full(len(lv_cables), 0.4)
        # asym_line["x0"] = np.full(len(lv_cables), 0.6)
        #asym_line["tan0"] = np.zeros(len(lv_cables))

        ID_counter += len(self.asym_line["id"])       
        
        #Initialize load array
        self.asym_load = initialize_array(DatasetType.input, ComponentType.asym_load, len(self.econnections))
        self.asym_load["id"] = np.array([i + ID_counter for i in range(len(self.econnections))])
        self.asym_load["node"] = Econ_node
        self.asym_load["status"] = np.ones(len(self.econnections))
        self.asym_load["type"] = [LoadGenType.const_power]
        self.asym_load["p_specified"] = [[1e3, 2e3, 0]] 
        self.asym_load["q_specified"] = [[0, 8e3, 2e3]] 
        
        ID_counter += len(self.asym_load["id"])

        #Initialize Source array
        source = initialize_array(DatasetType.input, ComponentType.source, len(self.source))
        source["id"] = np.array([i + ID_counter for i in range(len(self.source))]) #id's of line, nodes, sources and trafo are not allowed to be the same.
        source["node"] = source_to_node
        source["status"] = np.ones(len(self.source))
        source["u_ref"] = np.full(len(self.source), 1.0) 
        
        ID_counter += len(source["id"])


        mv_lv_trafo_props = {
            "u1" : 10000,
            "u2" : 400,
            "uk" : 0.025,
            "pk" : 1.0e3,
            "i0" : 1.0e-3,
            "p0" : 0.1,
            "winding_from" : 2,
            "winding_to" : 1,
            "clock" : 5,
            "tap_side" : 0, #0 = high voltage side  
            "tap_min" : 5,
            "tap_nom" : 3,
            "tap_max" : 1,
            "tap_size" : 0.25 * 1.0e3,
            "sn" : 4*1e6
        }
        #Initialize Transformer array
        self.transformer = initialize_array(DatasetType.input, ComponentType.transformer, len(self.get_assets_of_type(assets, esdl.Transformer)))
        self.transformer["id"] = np.array([i + ID_counter for i in range(len(self.transformer))])
        self.transformer["from_node"] = node_to_trafo
        self.transformer["to_node"] = trafo_to_node
        self.transformer["from_status"] = np.ones(len(self.transformer))
        self.transformer["to_status"] = np.ones(len(self.transformer))
        self.transformer["u1"] = [x* 1000 for x in primary_voltage]
        self.transformer["u2"] = [x* 1000 for x in secundary_voltage]
        self.transformer["uk"] = mv_lv_trafo_props["uk"] 
        self.transformer["pk"] = mv_lv_trafo_props["pk"] 
        self.transformer["i0"] = mv_lv_trafo_props["i0"] 
        self.transformer["p0"] = mv_lv_trafo_props["p0"] 
        self.transformer["winding_from"] = mv_lv_trafo_props["winding_from"]
        self.transformer["winding_to"] = mv_lv_trafo_props["winding_to"]
        self.transformer["clock"] = mv_lv_trafo_props["clock"]
        self.transformer["tap_side"] = mv_lv_trafo_props["tap_side"]
        self.transformer["tap_min"] = mv_lv_trafo_props["tap_min"]
        self.transformer["tap_nom"] = mv_lv_trafo_props["tap_nom"]
        self.transformer["tap_max"] = mv_lv_trafo_props["tap_max"]
        self.transformer["tap_size"] = mv_lv_trafo_props["tap_size"]
        self.transformer["sn"] = mv_lv_trafo_props["sn"]


        ID_counter += len(self.transformer["id"])

        # shunt = initialize_array(DatasetType.input, ComponentType.shunt, 1)
        # shunt["id"] = np.array([ID_counter])
        # shunt["node"] = trafo_to_node
        # shunt["g1"] = np.array([0.0])
        # shunt["b1"] = np.array([0.0])
        # shunt["g0"] = np.array([0.0])
        # shunt["b0"] = np.array([1/7])
        # shunt["status"] = np.array([1])

        input_data = {
            ComponentType.node: self.node,
            ComponentType.asym_line: self.asym_line,
            ComponentType.asym_load: self.asym_load,
            ComponentType.source: source #,
            #ComponentType.transformer: self.transformer,
            #ComponentType.shunt: shunt
        }
        assert_valid_input_data(input_data=input_data, calculation_type=CalculationType.power_flow)

        json_serialize_to_file(Path("Input_data.json"), input_data)
        self.model = PowerGridModel(input_data)
        #loadflowcalculation. symmetric must be false
        self.do_load_flow()
       

 

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
        
        #self.set_load_flow_parameters(param_dict)

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
        #LOGGER.debug('OpenDSS solve loadflow calculation')
        self.output_data = self.model.calculate_power_flow(
            symmetric=False, error_tolerance=1e-8, max_iterations=20, calculation_method=CalculationMethod.newton_raphson
        ) 

        print(self.output_data)
        # result dataset
        print("------node voltage result------")
        print(pd.DataFrame(self.output_data[ComponentType.node]["u"]))
        print("------node angle result------")
        print(pd.DataFrame(self.output_data[ComponentType.node]["u_angle"]))


    def process_results(self) -> PowerFlowResult: #here the results from the numpy arrays must be filled in
        # Process results
      
        json_serialize_to_file(Path("output_data.json"), self.output_data)


        # NODE RESULTS

        node_res = self.output_data[ComponentType.node]

        BusVoltageMag = node_res["u"]
        BusVoltageAng = node_res["u_angle"] * 180 / np.pi  # degrees

        node_ids = node_res["id"]
        node_id_to_index = {nid: i for i, nid in enumerate(node_ids)}

        rows = []

    
        # LINE RESULTS
 
        line_res = self.output_data[ComponentType.asym_line]

        P_from = line_res["p_from"]
        Q_from = line_res["q_from"]
        I_from = line_res["i_from"]

        P_to = line_res["p_to"]
        Q_to = line_res["q_to"]
        I_to = line_res["i_to"]

        for k in range(len(self.asym_line)):
            line_id = self.asym_line["id"][k]
            from_node = self.asym_line["from_node"][k]
            to_node   = self.asym_line["to_node"][k]

            from_idx = node_id_to_index[from_node]
            to_idx   = node_id_to_index[to_node]

            row = {
                "component_type": "line",
                "component_id": line_id,
                "from_node": from_node,
                "to_node": to_node,
            }

            for ph in range(3):
                S_from = P_from[k][ph] + 1j * Q_from[k][ph]
                S_to   = P_to[k][ph]   + 1j * Q_to[k][ph]

                # angles in DEGREES
                S_angle_from = np.angle(S_from) * 180 / np.pi
                S_angle_to   = np.angle(S_to)   * 180 / np.pi

                i_angle_from = BusVoltageAng[from_idx][ph] - S_angle_from
                i_angle_to   = BusVoltageAng[to_idx][ph]   - S_angle_to

                row.update({
                    f"U{ph+1}_from": BusVoltageMag[from_idx][ph],
                    f"U{ph+1}_from_angle": BusVoltageAng[from_idx][ph],
                    f"U{ph+1}_to": BusVoltageMag[to_idx][ph],
                    f"U{ph+1}_to_angle": BusVoltageAng[to_idx][ph],

                    f"I{ph+1}_from": I_from[k][ph],
                    f"I{ph+1}_from_angle": i_angle_from,
                    f"I{ph+1}_to": I_to[k][ph],
                    f"I{ph+1}_to_angle": i_angle_to,

                    f"P{ph+1}_from": P_from[k][ph],
                    f"P{ph+1}_to": P_to[k][ph],
                    f"Q{ph+1}_from": Q_from[k][ph],
                    f"Q{ph+1}_to": Q_to[k][ph],
                })

            rows.append(row)

        # LOAD RESULTS

        load_res = self.output_data[ComponentType.asym_load]

        P_load = load_res["p"]
        Q_load = load_res["q"]
        I_load = load_res["i"]

        for k in range(len(self.asym_load)):
            load_id = self.asym_load["id"][k]
            node_id = self.asym_load["node"][k]

            node_idx = node_id_to_index[node_id]

            row = {
                "component_type": "load",
                "component_id": load_id,
                "from_node": node_id,
                "to_node": None,
            }

            for ph in range(3):
                S = P_load[k][ph] + 1j * Q_load[k][ph]
                S_angle = np.angle(S) * 180 / np.pi

                i_angle = BusVoltageAng[node_idx][ph] - S_angle

                row.update({
                    f"U{ph+1}_from": BusVoltageMag[node_idx][ph],
                    f"U{ph+1}_from_angle": BusVoltageAng[node_idx][ph],

                    f"I{ph+1}_from": I_load[k][ph],
                    f"I{ph+1}_from_angle": i_angle,

                    f"P{ph+1}_from": P_load[k][ph],
                    f"Q{ph+1}_from": Q_load[k][ph],

                    f"U{ph+1}_to": np.nan,
                    f"U{ph+1}_to_angle": np.nan,
                    f"I{ph+1}_to": np.nan,
                    f"I{ph+1}_to_angle": np.nan,
                    f"P{ph+1}_to": np.nan,
                    f"Q{ph+1}_to": np.nan,
                })

            rows.append(row)

        df = pd.DataFrame(rows)
        df.to_excel("NetworkResults.xlsx", index=False)


 

        return PowerFlowResult(BusVoltageAng, BusVoltageMag)

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
