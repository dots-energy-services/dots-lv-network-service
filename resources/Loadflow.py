from dss import DSS as dss_engine
import networkx as nx
import seaborn as sns
import matplotlib.pyplot as plt
from plots import boxplot_real_generated
# ------------------
# OpenDSS simulation
# ------------------
# Define and compile network
print('Real')
dss_engine.Text.Command = f"compile test-3-real-knip.dss"

# Create a graph to represent the network
graph = nx.Graph()

# Add lines as edges in the graph
for l in range(dss_engine.ActiveCircuit.Lines.Count):
    dss_engine.ActiveCircuit.SetActiveElement(
        'Line.{0}'.format(dss_engine.ActiveCircuit.Lines.AllNames[l]))
    bus1 = dss_engine.ActiveCircuit.ActiveCktElement.BusNames[0].split('.')[0]
    bus2 = dss_engine.ActiveCircuit.ActiveCktElement.BusNames[1].split('.')[0]
    # print(bus1, bus2)
    r1 = dss_engine.ActiveCircuit.Lines.R1
    x1 = dss_engine.ActiveCircuit.Lines.X1
    impedance = (r1**2 + x1**2)**0.5
    graph.add_edge(bus1, bus2, weight=impedance)

# Calculate shortest path impedance from source
source_bus = "node1"
impedance_distances = nx.single_source_dijkstra_path_length(graph, source_bus, weight="weight")

# Print impedance distances
for bus, impedance in impedance_distances.items():
    print(f"Bus {bus}: Total Impedance Distance = {impedance:.4f} Ω")

# Solve load flow calculation
dss_engine.ActiveCircuit.Solution.Solve()

# # solve_time = time.time() - load_time
# LOGGER.info("Solve load flow calculation, took: ", {time.time() - start_time})
# start_time = time.time()


# Process results
BusNames = dss_engine.ActiveCircuit.AllBusNames
LineNames = dss_engine.ActiveCircuit.Lines.AllNames
BusVoltageMag = []
LineCurrentMag = []
LineCurrentAng = []
TotalLineCurrentMag = []
TotalLineCurrentLim = []
TransformerPower = []
TransformerPowerLim = []

# Phase voltage magnitudes for each bus:
for i in range(len(dss_engine.ActiveCircuit.AllBusVmag)):
    BusVoltageMag.append(round(dss_engine.ActiveCircuit.AllBusVmag[i], 2))
    # print(round(dss_engine.ActiveCircuit.AllBusVmag[i], 2))

# Phase current magnitudes and angles for each line:
for l in range(dss_engine.ActiveCircuit.Lines.Count):
    dss_engine.ActiveCircuit.SetActiveElement(
        'Line.{0}'.format(dss_engine.ActiveCircuit.Lines.AllNames[l]))
    # print('Line.{0}'.format(dss_engine.ActiveCircuit.Lines.AllNames[l]))
    Total_line_current = 0
    for i in range(1, 4):
        LineCurrentMag.append(
            round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2], 2))
        LineCurrentAng.append(
            round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2 + 1], 2))
        Total_line_current += round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2],
                                    2)
        # print(((dss_engine.ActiveCircuit.ActiveCktElement.TotalPowers[0])))
    TotalLineCurrentMag.append(Total_line_current)
    TotalLineCurrentLim.append(float(dss_engine.ActiveCircuit.ActiveCktElement.NormalAmps))

# for l in range(dss_engine.ActiveCircuit.Loads.Count):
#     dss_engine.ActiveCircuit.SetActiveElement('Load.{0}'.format(dss_engine.ActiveCircuit.Loads.AllNames[l]))
#     print(dss_engine.ActiveCircuit.ActiveCktElement.Name)
#     # print(dss_engine.ActiveCircuit.ActiveCktElement.Powers)
#     print(dss_engine.ActiveCircuit.ActiveCktElement.TotalPowers)

print(dss_engine.ActiveCircuit.TotalPower)

# # # print(dss_engine.ActiveCircuit.Lines.AllNames)
# # print(dss_engine.ActiveCircuit.AllNodeNames)
# # print(BusVoltageMag)
# print(dss_engine.ActiveCircuit.Lines.AllNames)
# print((TotalLineCurrentMag))
print(dss_engine.ActiveCircuit.LineLosses)

Cableloading_real = [(int(b)/int(m))*100 for b,m in zip(TotalLineCurrentMag, TotalLineCurrentLim)]
print(Cableloading_real)
# print(TotalLineCurrentMag)

# ------------------
# OpenDSS simulation
# ------------------
# Define and compile network
print('Generated')
dss_engine.Text.Command = f"compile test-3-knip.dss"

# Create a graph to represent the network
graph = nx.Graph()

# Add lines as edges in the graph
for l in range(dss_engine.ActiveCircuit.Lines.Count):
    dss_engine.ActiveCircuit.SetActiveElement(
        'Line.{0}'.format(dss_engine.ActiveCircuit.Lines.AllNames[l]))
    bus1 = dss_engine.ActiveCircuit.ActiveCktElement.BusNames[0].split('.')[0]
    bus2 = dss_engine.ActiveCircuit.ActiveCktElement.BusNames[1].split('.')[0]
    # print(bus1, bus2)
    r1 = dss_engine.ActiveCircuit.Lines.R1
    x1 = dss_engine.ActiveCircuit.Lines.X1
    impedance = (r1**2 + x1**2)**0.5
    graph.add_edge(bus1, bus2, weight=impedance)

# Calculate shortest path impedance from source
source_bus = "node1"
impedance_distances = nx.single_source_dijkstra_path_length(graph, source_bus, weight="weight")

# # Print impedance distances
# for bus, impedance in impedance_distances.items():
#     print(f"Bus {bus}: Total Impedance Distance = {impedance:.4f} Ω")

# Solve load flow calculation
dss_engine.ActiveCircuit.Solution.Solve()

# # solve_time = time.time() - load_time
# LOGGER.info("Solve load flow calculation, took: ", {time.time() - start_time})
# start_time = time.time()

# Process results
BusNames = dss_engine.ActiveCircuit.AllBusNames
LineNames = dss_engine.ActiveCircuit.Lines.AllNames
BusVoltageMag = []
LineCurrentMag = []
LineCurrentAng = []
TotalLineCurrentMag = []
TotalLineCurrentLim = []
TransformerPower = []
TransformerPowerLim = []
LinePower = []

# Phase voltage magnitudes for each bus:
for i in range(len(dss_engine.ActiveCircuit.AllBusVmag)):
    BusVoltageMag.append(round(dss_engine.ActiveCircuit.AllBusVmag[i], 2))
    # print(round(dss_engine.ActiveCircuit.AllBusVmag[i], 2))

# Phase current magnitudes and angles for each line:
for l in range(dss_engine.ActiveCircuit.Lines.Count):
    dss_engine.ActiveCircuit.SetActiveElement(
        'Line.{0}'.format(dss_engine.ActiveCircuit.Lines.AllNames[l]))
    # print('Line.{0}'.format(dss_engine.ActiveCircuit.Lines.AllNames[l]))
    Total_line_current = 0
    for i in range(1, 4):
        LineCurrentMag.append(
            round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2], 2))
        LineCurrentAng.append(
            round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2 + 1], 2))
        Total_line_current += round(dss_engine.ActiveCircuit.ActiveCktElement.CurrentsMagAng[(i - 1) * 2],
                                    2)
        # print(((dss_engine.ActiveCircuit.ActiveCktElement.TotalPowers[0])))

    TotalLineCurrentMag.append(Total_line_current)
    TotalLineCurrentLim.append(float(dss_engine.ActiveCircuit.ActiveCktElement.NormalAmps))
#
# for l in range(dss_engine.ActiveCircuit.Loads.Count):
#     dss_engine.ActiveCircuit.SetActiveElement('Load.{0}'.format(dss_engine.ActiveCircuit.Loads.AllNames[l]))
#     print(dss_engine.ActiveCircuit.ActiveCktElement.Name)
#     # print(dss_engine.ActiveCircuit.ActiveCktElement.Powers)
#     print(dss_engine.ActiveCircuit.ActiveCktElement.TotalPowers)

print(dss_engine.ActiveCircuit.TotalPower)
# # print(dss_engine.ActiveCircuit.Lines.AllNames)
# print(dss_engine.ActiveCircuit.AllNodeNames)
# print(BusVoltageMag)
# print(dss_engine.ActiveCircuit.Lines.AllNames)
# print((TotalLineCurrentMag))
print(dss_engine.ActiveCircuit.LineLosses)
# print(dss_engine.ActiveCircuit.AllElementLosses)
# print(dss_engine.ActiveCircuit.AllNodeDistances)

Cableloading_generated = [(int(b)/int(m))*100 for b,m in zip(TotalLineCurrentMag, TotalLineCurrentLim)]
print(Cableloading_generated)
# print(TotalLineCurrentMag)



boxplot_real_generated(Cableloading_real, Cableloading_generated)
# )
# sns.boxplot(Cableloading_real)

# plt.show()