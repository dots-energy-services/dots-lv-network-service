#!/usr/bin/env python

import typing
from base64 import b64decode

from dots_infrastructure.DataClasses import EsdlId
from esdl import esdl, EnergySystem
from esdl.esdl_handler import EnergySystemHandler

from dots_infrastructure.Logger import LOGGER

# RECEIVES_SERVICE_NAMES_LIST = [
#     "batteryservice",
#     "damarketservice",
#     "edemandservice",
#     "evservice",
#     "heatpumpservice",
#     "hybridheatpumpservice",
#     "pvsystemservice",
#     "weatherservice",
# ]

RECEIVES_SERVICE_NAMES_LIST = [
    "Battery",                  #batteryservice
    "EnergyMarket",             #damarketservice
    "ElectricityDemand",        #edemandservice
    "EVChargingStation",        #evservice
    "HeatPump",                 #heatpumpservice
    "HybridHeatPump",           #hybridheatpumpservice
    "PVInstallation",           #pvsystemservice
    "EnvironmentalProfiles",    #weatherservice
]


def get_energy_system(esdl_base64string: str) -> EnergySystem:
    esdl_string = b64decode(esdl_base64string + b"==".decode("utf-8")).decode("utf-8")
    esh = EnergySystemHandler()
    esh.load_from_string(esdl_string)
    return esh.get_energy_system()


def get_model_esdl_object(esdl_id: EsdlId, energy_system: EnergySystem) -> esdl:
    if energy_system.id == esdl_id:
        return energy_system
    # Iterate over all contents of the EnergySystem
    for obj in energy_system.eAllContents():
        if hasattr(obj, "id") and obj.id == esdl_id:
            return obj
    raise IOError(f"ESDL_ID '{esdl_id}' not found in provided ESDL file")


def get_connected_input_esdl_objects(
    esdl_id: EsdlId,
    calculation_services: typing.List[dict],
    energy_system: EnergySystem,
) -> dict[str, typing.List[EsdlId]]:
    model_esdl_obj = get_model_esdl_object(esdl_id, energy_system)

    connected_input_esdl_objects: dict[str, typing.List[EsdlId]] = {}
    if isinstance(model_esdl_obj, esdl.EnergyAsset):
        add_calc_services_from_ports(
            calculation_services, connected_input_esdl_objects, model_esdl_obj
        )
        add_calc_services_from_non_connected_objects(
            calculation_services, connected_input_esdl_objects, energy_system
        )
        add_calc_services_from_building_objects(
            calculation_services, connected_input_esdl_objects, model_esdl_obj
        )
    else:
        add_calc_services_from_all_objects(
            calculation_services, connected_input_esdl_objects, energy_system
        )
    return connected_input_esdl_objects


def add_calc_services_from_ports(
    calculation_services: typing.List[dict],
    connected_input_esdl_objects: dict,
    model_esdl_asset: esdl.EnergyAsset,
):
    # Iterate over all ports of this asset
    for port in model_esdl_asset.port:
        # only InPorts to find connected receiving services
        if isinstance(port, esdl.InPort):
            # Iterate over all connected ports of this port
            for connected_port in port.connectedTo:
                # Get the asset to which the connected port belongs to
                connected_asset = connected_port.eContainer()
                add_esdl_object(
                    connected_input_esdl_objects, connected_asset, calculation_services
                )


def add_calc_services_from_non_connected_objects(
    calculation_services: typing.List[dict],
    connected_input_esdl_objects: dict,
    energy_system: esdl,
):
    for esdl_obj in energy_system.eAllContents():
        if not isinstance(esdl_obj, esdl.EnergyAsset) and hasattr(esdl_obj, "id"):
            add_esdl_object(
                connected_input_esdl_objects, esdl_obj, calculation_services
            )
    add_esdl_object(connected_input_esdl_objects, energy_system, calculation_services)


def add_calc_services_from_all_objects(
    calculation_services: typing.List[dict],
    connected_input_esdl_objects: dict,
    energy_system: esdl.EnergySystem,
):
    for esdl_obj in energy_system.eAllContents():
        if hasattr(esdl_obj, "id"):
            add_esdl_object(
                connected_input_esdl_objects, esdl_obj, calculation_services
            )

def add_calc_services_from_building_objects(
    calculation_services: typing.List[dict],
    connected_input_esdl_objects: dict,
    model_esdl_asset: esdl.EnergyAsset
):
    if isinstance(model_esdl_asset.eContainer(), esdl.Building):
        building = model_esdl_asset.eContainer()
        for esdl_obj in building.asset:
            if hasattr(esdl_obj, "id"):
                add_esdl_object(connected_input_esdl_objects, esdl_obj, calculation_services)

def add_esdl_object(
    connected_input_esdl_objects: dict,
    esdl_obj: esdl,
    calculation_services: typing.List[dict],
):
    # find calculation service for ESDL object type
    calc_service = next(
        (
            calc_service
            for calc_service in calculation_services
            if calc_service == type(esdl_obj).__name__),
        None,
    )

    if (
        calc_service
        and calc_service in RECEIVES_SERVICE_NAMES_LIST
    ):
        service_name = calc_service
        esdl_id = f"{str(esdl_obj.id)}"
        if service_name not in tuple(connected_input_esdl_objects):
            connected_input_esdl_objects[service_name] = [esdl_id]
        elif esdl_id not in connected_input_esdl_objects[service_name]:
            connected_input_esdl_objects[service_name].append(esdl_id)
    else:
        LOGGER.debug(
            f"No calculation service found for ESDL type '{type(esdl_obj).__name__}'"
        )
