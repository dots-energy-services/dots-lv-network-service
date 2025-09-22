import re
import dss
from dss import IDSS
import numpy as np
import pandas as pd

from power_grid_model.validation import assert_valid_input_data
from power_grid_model import (
    CalculationMethod,
    CalculationType,
    ComponentType,
    DatasetType,
    LoadGenType,
    PowerGridModel,
    initialize_array
)

def get_c_values(dss_engine : IDSS, line_code_name : str) -> tuple[float, float, float]:
    
    result = dss_engine.ActiveCircuit.LineCodes.First
    while result != 0:
        if dss_engine.ActiveCircuit.LineCodes.Name == line_code_name:
            c0 = dss_engine.ActiveCircuit.LineCodes.C0
            c1 = dss_engine.ActiveCircuit.LineCodes.C1
            normaps = dss_engine.ActiveCircuit.LineCodes.NormAmps
            return c0, c1, normaps
        result = dss_engine.ActiveCircuit.LineCodes.Next
    raise ValueError(f"Line code {line_code_name} not found")


def extract_numbers(text : str):
    # This regex finds integers and decimal numbers
    numbers = re.findall(r'\d+\.?\d*', text)
    return [float(num) if '.' in num else int(num) for num in numbers]

def get_bus_from_bus(busses_str : str, filter_bus_out : str) -> str:
    # [joint5, lvjoint7.1.2.3, ]
    busses_str = busses_str.replace('[', '').replace(']', '')
    busses_str_split = busses_str.split(', ')
    all_busses = [bus for bus in busses_str_split if bus != '' and bus != filter_bus_out]
    if len(all_busses) != 1:
        raise ValueError(f"Expected exactly one bus different from {filter_bus_out}, but got {all_busses}")
    return all_busses[0]

dss_engine = dss.DSS
dss_engine.Text.Command = f"compile pgm_test/mv-energy-system.dss"
dss_engine.ActiveCircuit.Solution.Solve()

property_mapping = {
    "kV" : 0
}

dss_engine.ActiveCircuit.SetActiveElement(dss_engine.ActiveCircuit.AllNodeNames[0])
for i, prop_name in enumerate(dss_engine.ActiveCircuit.ActiveCktElement.AllPropertyNames):
    if prop_name in property_mapping:
        property_mapping[prop_name] = i

id = 0
node_ids = []
node_rated_u = []
dss_name_to_id = {}
for name in dss_engine.ActiveCircuit.AllNodeNames:
    active_ckt_element = dss_engine.ActiveCircuit.ActiveCktElement
    u_rated_v = float(active_ckt_element.Properties[property_mapping["kV"]].Val) * 1000
    name_without_phases = name.split('.')[0]
    if name_without_phases not in dss_name_to_id:
        id += 1
        dss_name_to_id[name_without_phases] = id
        node_ids.append(id)
        node_rated_u.append(u_rated_v)

property_mapping = {
    "C0" : 0,
    "C1" : 0,
    "LineCode" : 0,
    "RMatrix" : 0,
    "XMatrix" : 0,
    "Length" : 0,
    "Bus1" : 0,
    "Bus2" : 0,
    "CMatrix" : 0
}

line_ids = []
r_aa = []
r_ba = []
r_bb = []
r_ca = []
r_cb = []
r_cc = []
r_na = []
r_nb = []
r_nc = []
r_nn = []
r_value_list = [r_aa, r_ba, r_bb, r_ca, r_cb, r_cc, r_na, r_nb, r_nc, r_nn]
x_aa = []
x_ba = []
x_bb = []
x_ca = []
x_cb = []
x_cc = []
x_na = []
x_nb = []
x_nc = []
x_nn = []
x_value_list = [x_aa, x_ba, x_bb, x_ca, x_cb, x_cc, x_na, x_nb, x_nc, x_nn]
c0_values = []
c1_values = []
line_from_node = []
line_to_node = []

dss_engine.ActiveCircuit.SetActiveElement(f"Line.{dss_engine.ActiveCircuit.Lines.AllNames[0]}")
for i, prop_name in enumerate(dss_engine.ActiveCircuit.ActiveCktElement.AllPropertyNames):
    if prop_name in property_mapping:
        property_mapping[prop_name] = i

for name in dss_engine.ActiveCircuit.Lines.AllNames:
    result = dss_engine.ActiveCircuit.SetActiveElement(f'Line.{name}')
    active_ckt_element = dss_engine.ActiveCircuit.ActiveCktElement
    property_values = {}
    for key, index in property_mapping.items():
        if key == "Bus1" or key == "Bus2":
            bus_name = active_ckt_element.Properties[index].Val.split('.')[0]
            property_values[key] = dss_name_to_id[bus_name]
        else:
            property_values[key] = active_ckt_element.Properties[index].Val
    c0, c1, normaps = get_c_values(dss_engine, property_values["LineCode"])
    r_matrix_values = [x * float(property_values["Length"]) for x in extract_numbers(property_values["RMatrix"])]
    x_matrix_values = [x  * float(property_values["Length"]) for x in extract_numbers(property_values["XMatrix"])]
    c0_values.append(c0 * 1.0e-9)
    c1_values.append(c1 * 1.0e-9)
    line_from_node.append(property_values["Bus1"])
    line_to_node.append(property_values["Bus2"])
    for i, val in enumerate(r_matrix_values):
        r_value_list[i].append(val)
    for i, val in enumerate(x_matrix_values):
        x_value_list[i].append(val)
    if len(r_matrix_values) != len(r_value_list):
        r_na.append(np.nan)
        r_nb.append(np.nan)
        r_nc.append(np.nan)
        r_nn.append(np.nan)
    if len(x_matrix_values) != len(x_value_list):
        x_na.append(np.nan)
        x_nb.append(np.nan)
        x_nc.append(np.nan)
        x_nn.append(np.nan)
    id += 1 
    line_ids.append(id)

high_voltage_trafo_props = {
    "u1" : 150 * 1.0e3,
    "u2" : 11 * 1.0e3,
    "uk" : 0.175,
    "pk" : 196*1.0e3,
    "i0" : 21,
    "p0" : 34.3 *1.0e3,
    "winding_from" : 0,
    "winding_to" : 2,
    "clock" : 5,
    "tap_side" : 1,
    "tap_min" : -13,
    "tap_nom" : 0,
    "tap_max" : 9,
    "tap_size" : 2.5 * 1.0e3,
    "sn" : 40*10e6
}

mv_lv_trafo_props = {
    "u1" : 10750,
    "u2" : 400,
    "uk" : 0.175,
    "pk" : 196*1.0e3,
    "i0" : 5.77350269,
    "p0" : 0.515*1.0e3,
    "winding_from" : 2,
    "winding_to" : 1,
    "clock" : 5,
    "tap_side" : 1,
    "tap_min" : 5,
    "tap_nom" : 3,
    "tap_max" : 1,
    "tap_size" : 0.25 * 1.0e3,
    "sn" : 400*10e3
}

trafo_ids = []
trafo_u1 = []
trafo_u2 = []
trafo_uk = []
trafo_pk = []
trafo_i0 = []
trafo_p0 = []
trafo_winding_from = []
trafo_winding_to = []
trafo_clock = []
trafo_tap_side = []
trafo_tap_min = []
trafo_tap_max = []
trafo_tap_size = []
trafo_tap_nom = []
trafo_from_node = []
trafo_to_node = []
trafo_sn = []
result = dss_engine.ActiveCircuit.Transformers.First

bal = dss_engine.ActiveCircuit.SetActiveElement("Transformer.transformer5")
tal = dss_engine.ActiveCircuit.ActiveCktElement
kla = 0

transformer_property_mapping = {
    "XfmrCode" : 0,
    "Bus" : 0,
    "Buses" : 0
}
dss_engine.ActiveCircuit.SetActiveElement(f"Transformer.{dss_engine.ActiveCircuit.Transformers.AllNames[0]}")
for i, prop_name in enumerate(dss_engine.ActiveCircuit.ActiveCktElement.AllPropertyNames):
    if prop_name in transformer_property_mapping:
        transformer_property_mapping[prop_name] = i

for name in dss_engine.ActiveCircuit.Transformers.AllNames:
    result = dss_engine.ActiveCircuit.SetActiveElement(f"Transformer.{name}")
    active_transformer = dss_engine.ActiveCircuit.ActiveCktElement
    property_values = {}
    for key, index in transformer_property_mapping.items():
        property_values[key] = active_ckt_element.Properties[index].Val
    trafo_properties = mv_lv_trafo_props
    if property_values["XfmrCode"] == 'highvoltagetesttrafotype':
        trafo_properties = high_voltage_trafo_props

    id += 1
    trafo_u1.append(trafo_properties["u1"])
    trafo_u2.append(trafo_properties["u2"])
    trafo_uk.append(trafo_properties["uk"])
    trafo_pk.append(trafo_properties["pk"])
    trafo_i0.append(trafo_properties["i0"])
    trafo_p0.append(trafo_properties["p0"])
    trafo_winding_from.append(trafo_properties["winding_from"])
    trafo_winding_to.append(trafo_properties["winding_from"])
    trafo_clock.append(trafo_properties["clock"])
    trafo_tap_side.append(trafo_properties["tap_side"])
    trafo_tap_min.append(trafo_properties["tap_min"])
    trafo_tap_nom.append(trafo_properties["tap_nom"])
    trafo_tap_max.append(trafo_properties["tap_max"])
    trafo_tap_size.append(trafo_properties["tap_size"])
    trafo_sn.append(trafo_properties["sn"])
    trafo_ids.append(id)

    to_node_bus = property_values["Bus"].split('.')[0]
    from_node_bus = get_bus_from_bus(property_values["Buses"], property_values["Bus"])
    trafo_from_node.append(dss_name_to_id[from_node_bus])
    trafo_to_node.append(dss_name_to_id[to_node_bus])

load_property_mapping = {
    "Bus1" : 0
}
dss_engine.ActiveCircuit.SetActiveElement(f"Load.{dss_engine.ActiveCircuit.Loads.AllNames[0]}")
for i, prop_name in enumerate(dss_engine.ActiveCircuit.ActiveCktElement.AllPropertyNames):
    if prop_name in load_property_mapping:
        load_property_mapping[prop_name] = i

load_names_captured = []
load_nodes = []
for name in dss_engine.ActiveCircuit.Loads.AllNames:
    result = dss_engine.ActiveCircuit.SetActiveElement(f'Load.{name}')
    active_ckt_element = dss_engine.ActiveCircuit.ActiveCktElement
    property_values = {}
    for key, index in load_property_mapping.items():
        property_values[key] = active_ckt_element.Properties[index].Val
    load_name = name.split('_')[0]
    if load_name not in load_names_captured:
        load_nodes.append(dss_name_to_id[property_values["Bus1"].split('.')[0]])
        load_names_captured.append(load_name)



node = initialize_array(DatasetType.input, ComponentType.node, len(node_ids))
node["id"] = node_ids
node["u_rated"] = node_rated_u

# load
id = id + 1
amount_of_loads = int(dss_engine.ActiveCircuit.Loads.Count / 3)
load_ids = list(range(id, id + amount_of_loads))
asym_load = initialize_array(DatasetType.input, ComponentType.asym_load, amount_of_loads)
asym_load["id"] = load_ids
asym_load["node"] = load_nodes
asym_load["status"] = [1 for i in range(amount_of_loads)]
asym_load["type"] = [LoadGenType.const_power]
asym_load["p_specified"] = [[1e3,1e3,1e3]]
asym_load["q_specified"] = [[0,0,0]]

# source
id = max(load_ids) + 1
source = initialize_array(DatasetType.input, ComponentType.source, 1)
source["id"] = [id]
source["node"] = [dss_name_to_id["jointhighvoltagetrafo_import"]]
source["status"] = [1]
source["u_ref"] = [150*1.0e3]

# transformer
transformer = initialize_array(DatasetType.input, ComponentType.transformer, len(trafo_ids))
transformer["id"] = trafo_ids
transformer["from_node"] = trafo_from_node
transformer["to_node"] = trafo_to_node
transformer["from_status"] = [1 for i in range(len(trafo_ids))]
transformer["to_status"] = [1 for i in range(len(trafo_ids))]
transformer["u1"] = trafo_u1
transformer["u2"] = trafo_u2
transformer["sn"] = trafo_sn
transformer["uk"] = trafo_uk
transformer["pk"] = trafo_pk
transformer["i0"] = trafo_i0
transformer["p0"] = trafo_p0
transformer["winding_from"] = trafo_winding_from
transformer["winding_to"] = trafo_winding_to
transformer["clock"] = trafo_clock
transformer["tap_side"] = trafo_tap_side
transformer["tap_min"] = trafo_tap_min
transformer["tap_max"] = trafo_tap_max
transformer["tap_size"] = trafo_tap_size
transformer["tap_nom"] = trafo_tap_nom

asym_line = initialize_array(DatasetType.input, ComponentType.asym_line, len(line_ids))
asym_line["id"] = line_ids
asym_line["from_node"] = line_from_node
asym_line["to_node"] = line_to_node
asym_line["from_status"] = [1 for i in range(len(line_ids))]
asym_line["to_status"] = [1 for i in range(len(line_ids))]
asym_line["r_aa"] = r_aa
asym_line["r_ba"] = r_ba
asym_line["r_bb"] = r_bb
asym_line["r_ca"] = r_ca
asym_line["r_cb"] = r_cb
asym_line["r_cc"] = r_cc
asym_line["r_na"] = r_na
asym_line["r_nb"] = r_nb
asym_line["r_nc"] = r_nc
asym_line["r_nn"] = r_nn
asym_line["x_aa"] = x_aa
asym_line["x_ba"] = x_ba
asym_line["x_bb"] = x_bb
asym_line["x_ca"] = x_ca
asym_line["x_cb"] = x_cb
asym_line["x_cc"] = x_cc
asym_line["x_na"] = x_na
asym_line["x_nb"] = x_nb
asym_line["x_nc"] = x_nc
asym_line["x_nn"] = x_nn
asym_line["c0"] = c0_values
asym_line["c1"] = c1_values

input_data = {
    ComponentType.node: node,
    ComponentType.asym_line: asym_line,
    ComponentType.asym_load: asym_load,
    ComponentType.source: source,
    ComponentType.transformer: transformer
}


assert_valid_input_data(input_data=input_data, calculation_type=CalculationType.power_flow)

# construction
model = PowerGridModel(input_data)

# one-time power flow calculation
output_data = model.calculate_power_flow(
    symmetric=False, error_tolerance=1e-8, max_iterations=20, calculation_method=CalculationMethod.newton_raphson
)

bal = 0