from pathlib import Path
from power_grid_model import PowerGridModel
from power_grid_model.utils import import_input_data
from power_grid_model import (
    CalculationMethod
)


pgm = PowerGridModel(input_data=import_input_data(Path("out.json")))
output_data = pgm.calculate_power_flow(symmetric=False, error_tolerance=1e-8, max_iterations=20, calculation_method=CalculationMethod.newton_raphson)
print(output_data["node"])