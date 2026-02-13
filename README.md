# Calculation service for esdl_type EnergySystem:

This calculation service performs a load flow calculation based upon the active and reactive power dispatch for all of the household connections. The service uses OpenDSS as power flow calculation engine

## Calculations

### load_flow_current_step 

Calculate load flow for the current timestep.
#### Input parameters
|Name            |esdl_type            |data_type            |unit            |description            |
|----------------|---------------------|---------------------|----------------|-----------------------|
|aggregated_active_power|EConnection|VECTOR|W|The aggregated active power for the EConnection.|
|aggregated_reactive_power|EConnection|VECTOR|W|The aggregated reactive power for the EConnection.|

### Relevant links
|Link             |description             |
|-----------------|------------------------|
|[EnergySystem](https://energytransition.github.io/#router/doc-content/687474703a2f2f7777772e746e6f2e6e6c2f6573646c/EnergySystem.html)|Details on the EnergySystem esdl type|
|[dss-python](https://pypi.org/project/dss-python/)|Details on the dss-python package to use OpenDSS load flow calculations|
