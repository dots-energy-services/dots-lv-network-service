# Load OpenDSS
import dss

# Run a DSS script to load a circuit
dss_engine = dss.DSS
dss_engine.Text.Command = f"compile main.dss"
dss_engine.ActiveCircuit.Solution.Solve()

active_circuit = dss_engine.ActiveCircuit
active_circuit.SetActiveElement("Transformer.transformer1")
active_transformer = active_circuit.ActiveElement
active_transformer.Powers
# or dss.Text.Command('Redirect "./../../tests/data/13Bus/IEEE13Nodeckt.dss"')

# Select a load and update its kW
# dss.Loads.Name("675c")
# dss.Loads.kW(320)

# Solve
bal = 5