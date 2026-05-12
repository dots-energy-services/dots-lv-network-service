<?xml version='1.0' encoding='UTF-8'?>
<esdl:EnergySystem xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:esdl="http://www.tno.nl/esdl" name="test-mv-network" id="3bce4948-af6f-4f4c-bb6b-56a96f71e334" description="Test mv network ">
  <services xsi:type="esdl:Services">
    <service xsi:type="esdl:EnergyMarket" name="global market" id="ff6afcf9-a2a0-4c68-807c-845638078bb0"/>
  </services>
  <instance xsi:type="esdl:Instance" name="Mv network" id="3e2ac56e-169c-45d8-b325-6ce81a4a7a57">
    <area xsi:type="esdl:Area" id="446807ea-3d58-4fdb-a76a-40ec10bbc850" name="MV Network name">
      <asset xsi:type="esdl:Import" name="root" assetType="50.0" id="846ee735-783b-4201-801c-ba77bf7abd92">
        <port xsi:type="esdl:OutPort" id="8f6bf2ab-c39d-4c06-bf10-72b39dbfc1e9" name="Out" connectedTo="fdb0231e-2745-49ac-97a9-6e16d297ae79"/>
        <geometry xsi:type="esdl:Point" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:Joint" name="jointhighvoltagetrafo_import" id="1989defa-1ee5-4037-a69c-8f3e548dd997">
        <port xsi:type="esdl:InPort" id="fdb0231e-2745-49ac-97a9-6e16d297ae79" name="In" connectedTo="8f6bf2ab-c39d-4c06-bf10-72b39dbfc1e9"/>
        <port xsi:type="esdl:OutPort" id="2010a412-d649-402f-bd02-a4ef6d794a8f" name="Out" connectedTo="6b9466eb-477c-4831-ae8d-88430046516e"/>
        <geometry xsi:type="esdl:Point" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:Transformer" voltagePrimary="50.0" commissioningDate="2000-01-01T00:00:00.000000" name="highvoltagetrafo" assetType="highvoltagetesttrafotype" id="a7679301-75c6-4665-983a-c47243bddda9" voltageSecundary="10.0">
        <port xsi:type="esdl:InPort" id="6b9466eb-477c-4831-ae8d-88430046516e" name="In" connectedTo="2010a412-d649-402f-bd02-a4ef6d794a8f"/>
        <port xsi:type="esdl:OutPort" id="45e53c4a-d2ea-4862-933f-9d6f45fc4a2c" name="Out" connectedTo="afd113b3-6ef7-4714-9871-c643f4d49066"/>
        <geometry xsi:type="esdl:Point" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:Joint" name="jointhighvoltagetrafo" id="251a61f6-e739-4088-ac8d-3d4db626593f">
        <port xsi:type="esdl:InPort" id="afd113b3-6ef7-4714-9871-c643f4d49066" name="In" connectedTo="45e53c4a-d2ea-4862-933f-9d6f45fc4a2c"/>
        <port xsi:type="esdl:OutPort" id="7eb4331f-11c0-4820-a9d5-454e90c7c06b" name="Out" connectedTo="3c91b157-b108-4a0a-8b54-d1802e0295d3 ffc32c26-7914-426f-931c-adc79d0de968"/>
        <geometry xsi:type="esdl:Point" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="mv_cable_1" length="13.416407864998739" assetType="mv_line" id="75df072a-fc47-4ed2-9adc-80e61fd5c728">
        <port xsi:type="esdl:InPort" id="3c91b157-b108-4a0a-8b54-d1802e0295d3" name="In" connectedTo="7eb4331f-11c0-4820-a9d5-454e90c7c06b"/>
        <port xsi:type="esdl:OutPort" id="6322957d-7d7c-42e7-af17-4f8c5e98c69f" name="Out" connectedTo="de809879-f6fb-44ce-ab47-0a97705ec761"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="6.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="mv_side_joint1" id="5d32d73d-48e5-4ae2-9f57-a7c5520c6198">
        <port xsi:type="esdl:InPort" id="de809879-f6fb-44ce-ab47-0a97705ec761" name="In" connectedTo="6322957d-7d7c-42e7-af17-4f8c5e98c69f"/>
        <port xsi:type="esdl:OutPort" id="2b8ddd6e-d85f-4e11-ae28-4caf65627a28" name="Out" connectedTo="e40cdfb6-f297-4886-82ee-7fe6f4d57d39 e41dce49-040a-437f-a47b-1cec6040a1e9"/>
        <geometry xsi:type="esdl:Point" lat="6.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:Transformer" voltagePrimary="10.0" commissioningDate="2000-01-01T00:00:00.000000" name="first-mv-trafo" assetType="testtrafotype" id="11d2e6a3-cffb-4999-ae25-c2899ec4f51b" voltageSecundary="0.4">
        <port xsi:type="esdl:InPort" id="e41dce49-040a-437f-a47b-1cec6040a1e9" name="In" connectedTo="2b8ddd6e-d85f-4e11-ae28-4caf65627a28"/>
        <port xsi:type="esdl:OutPort" id="55bc2d67-0ba0-46a4-9b81-bc9e31d2d4e4" name="Out" connectedTo="43f98a7a-5c45-4b34-930c-8e7832cfc166"/>
        <geometry xsi:type="esdl:Point" lat="6.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="mv_cable_2" length="13.416407864998739" assetType="mv_line" id="31dc0600-5a3c-48c7-b78d-a9dd757e5a65">
        <port xsi:type="esdl:InPort" id="e40cdfb6-f297-4886-82ee-7fe6f4d57d39" name="In" connectedTo="2b8ddd6e-d85f-4e11-ae28-4caf65627a28"/>
        <port xsi:type="esdl:OutPort" id="a8ff6dcd-5dc3-4e68-b34e-28c909cf9c80" name="Out" connectedTo="a7cc53a8-863f-4407-8eea-aee0a529e11a"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="6.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="12.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="mv_cable_4" length="18.0" assetType="mv_line" id="0ce9bcfd-940f-40a3-bdb5-974b9e973816">
        <port xsi:type="esdl:InPort" id="95b5bcef-943a-478d-9e9a-a6eb7aa1004d" name="In" connectedTo="efa776f4-4e7b-486b-8d90-43c3e3506740"/>
        <port xsi:type="esdl:OutPort" id="aa69c33f-64b5-4b0b-b5f0-9952f9af970c" name="Out" connectedTo="3b587df8-39ac-4650-a446-af6b8ff9ab0c"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="12.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="12.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="mv_side_joint2" id="e23cbad6-93f6-40ab-b24a-861344612068">
        <port xsi:type="esdl:InPort" id="a7cc53a8-863f-4407-8eea-aee0a529e11a" name="In" connectedTo="a8ff6dcd-5dc3-4e68-b34e-28c909cf9c80"/>
        <port xsi:type="esdl:OutPort" id="efa776f4-4e7b-486b-8d90-43c3e3506740" name="Out" connectedTo="95b5bcef-943a-478d-9e9a-a6eb7aa1004d a5265e26-0f4e-4e3c-a8e1-ee40688f2e22"/>
        <geometry xsi:type="esdl:Point" lat="12.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:Transformer" voltagePrimary="10.0" commissioningDate="2000-01-01T00:00:00.000000" name="second-mv-trafo" assetType="testtrafotype" id="5d6f85e8-8feb-44de-a550-7caf5fe93d67" voltageSecundary="0.4">
        <port xsi:type="esdl:InPort" id="a5265e26-0f4e-4e3c-a8e1-ee40688f2e22" name="In" connectedTo="efa776f4-4e7b-486b-8d90-43c3e3506740"/>
        <port xsi:type="esdl:OutPort" id="dc8ee388-1d69-4a96-b81a-a6608bd56f1b" name="Out" connectedTo="efe346e3-f264-4285-b8a9-d4b0e8383adf"/>
        <geometry xsi:type="esdl:Point" lat="12.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="mv_cable_3" length="13.416407864998739" assetType="mv_line" id="e95e0e2f-ca24-47c4-b45a-cd4e36716a6d">
        <port xsi:type="esdl:InPort" id="ffc32c26-7914-426f-931c-adc79d0de968" name="In" connectedTo="7eb4331f-11c0-4820-a9d5-454e90c7c06b"/>
        <port xsi:type="esdl:OutPort" id="f2212d0c-d579-4ef3-9147-432008ae6ab3" name="Out" connectedTo="3b587df8-39ac-4650-a446-af6b8ff9ab0c"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="12.0" lon="6.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="mv_side_joint3" id="09260aa0-999b-46b7-99db-b73445883d60">
        <port xsi:type="esdl:InPort" id="3b587df8-39ac-4650-a446-af6b8ff9ab0c" name="In" connectedTo="f2212d0c-d579-4ef3-9147-432008ae6ab3 aa69c33f-64b5-4b0b-b5f0-9952f9af970c"/>
        <port xsi:type="esdl:OutPort" id="5a17b09a-006a-4977-89c6-c91f774120aa" name="Out" connectedTo="1f4730e2-7015-45f6-b4ab-9c42e821ff79"/>
        <geometry xsi:type="esdl:Point" lat="12.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:Transformer" voltagePrimary="10.0" commissioningDate="2000-01-01T00:00:00.000000" name="third-mv-trafo" assetType="testtrafotype" id="1d24dfad-9939-4c1a-b183-8096d4aa23b8" voltageSecundary="0.4">
        <port xsi:type="esdl:InPort" id="1f4730e2-7015-45f6-b4ab-9c42e821ff79" name="In" connectedTo="5a17b09a-006a-4977-89c6-c91f774120aa"/>
        <port xsi:type="esdl:OutPort" id="09554465-b09d-404a-bab0-fab02c7e3aba" name="Out" connectedTo="4157bd78-69f4-4550-8718-2639d562350d"/>
        <geometry xsi:type="esdl:Point" lat="12.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:AggregatedConsumer" name="agg_consumer_3" id="ed6e29d3-071a-483d-95b2-b904038bec09">
        <port xsi:type="esdl:InPort" id="4157bd78-69f4-4550-8718-2639d562350d" name="In" connectedTo="09554465-b09d-404a-bab0-fab02c7e3aba"/>
        <port xsi:type="esdl:OutPort" id="3b969bbe-9749-4040-8ea6-8df70ac31e2c" name="Out" connectedTo="134d60e5-103b-4148-a464-40a01424a826"/>
        <geometry xsi:type="esdl:Point" lat="12.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:AggregatedConsumer" name="agg_consumer_1" id="2e30af5b-9882-413c-b1fe-c2225c8194ca">
        <port xsi:type="esdl:InPort" id="43f98a7a-5c45-4b34-930c-8e7832cfc166" name="In" connectedTo="55bc2d67-0ba0-46a4-9b81-bc9e31d2d4e4"/>
        <port xsi:type="esdl:OutPort" id="6f89cccc-af48-4c27-82a5-4c7a065809d7" name="Out" connectedTo="41fe6786-26c8-486b-8a9f-62c68ba43abe"/>
        <geometry xsi:type="esdl:Point" lat="6.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:AggregatedConsumer" name="agg_consumer_2" id="2aa7c113-b3e0-411c-92ea-fe8b8c1490a0">
        <port xsi:type="esdl:InPort" id="efe346e3-f264-4285-b8a9-d4b0e8383adf" name="In" connectedTo="dc8ee388-1d69-4a96-b81a-a6608bd56f1b"/>
        <port xsi:type="esdl:OutPort" id="970831cf-9cff-4f9d-95f2-a2709cedf914" name="Out" connectedTo="30255e01-8679-4aea-a267-b1704a747862"/>
        <geometry xsi:type="esdl:Point" lat="12.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable0" length="6.0" assetType="lv_line" id="ff2ad61a-79e5-4de0-b1ba-b663bfb190d2">
        <port xsi:type="esdl:InPort" id="41fe6786-26c8-486b-8a9f-62c68ba43abe" name="In" connectedTo="6f89cccc-af48-4c27-82a5-4c7a065809d7"/>
        <port xsi:type="esdl:OutPort" id="5e0129a4-eaab-4f56-9d30-859fa2e4dadf" name="Out" connectedTo="6d88f873-9b8c-438e-9473-a1a318d2c386"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="6.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="12.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint0" id="a079e5f6-f2d2-4312-9f73-280102fe78f1">
        <port xsi:type="esdl:InPort" id="6d88f873-9b8c-438e-9473-a1a318d2c386" name="In" connectedTo="5e0129a4-eaab-4f56-9d30-859fa2e4dadf"/>
        <port xsi:type="esdl:OutPort" id="9d02b99a-e188-45fd-9cba-30a8bb36590d" name="Out" connectedTo="f53c2217-d494-4203-9fb7-6c235fbaf520 54dfeeb1-74a7-447e-afa5-6d866dced577 ea583250-e067-47d3-8abc-79f4124ac528"/>
        <geometry xsi:type="esdl:Point" lat="12.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home0-1" length="1.0" assetType="lv_line_to_home" id="aa26555e-b76b-4474-b8f2-c83ecd88bf25">
        <port xsi:type="esdl:InPort" id="f53c2217-d494-4203-9fb7-6c235fbaf520" name="In" connectedTo="9d02b99a-e188-45fd-9cba-30a8bb36590d"/>
        <port xsi:type="esdl:OutPort" id="4ba8d249-eb5e-4f70-b4fc-650d55315caa" name="Out" connectedTo="16bfad39-c37b-4265-bfcd-3025e83290f5"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="12.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="12.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home0-2" length="1.0" assetType="lv_line_to_home" id="41d509ac-a2f0-4aaf-9293-c604fbfc4edf">
        <port xsi:type="esdl:InPort" id="54dfeeb1-74a7-447e-afa5-6d866dced577" name="In" connectedTo="9d02b99a-e188-45fd-9cba-30a8bb36590d"/>
        <port xsi:type="esdl:OutPort" id="acfa05a4-d7dc-4d1c-8ebb-f1fb368d4246" name="Out" connectedTo="276bca73-9b03-4ddf-b93d-7248fd3ca260"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="12.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="12.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home0-1" id="17fc88dc-4b5c-4b46-89b7-dba93a69e25b">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="408e516f-ff1d-4cd9-b8ef-f9788341e444" connectedTo="3292d6a6-38b7-48e5-b3da-2148ff7e6537" name="In"/>
          <port xsi:type="esdl:OutPort" id="d8254743-7ff1-475a-87f3-995b44f8ff97" name="Out" connectedTo="beb4dd8b-ba3c-475d-947c-bc8e2c15210b"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="2e416b2b-d5f9-4ab9-ac16-c21c3a67551a" connectedTo="44068fe8-a93c-4b10-b660-a44fe2c39a2e" name="In"/>
          <port xsi:type="esdl:OutPort" id="75396de5-1ec0-47b0-843e-fdab3dd78975" name="Out" connectedTo="29c1678c-5c22-4068-876b-97436af468ae"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="f02090db-e48f-4bf6-b744-0226bb0c1af2" connectedTo="332b29ad-0588-4f3c-b553-268ceb308370" name="In"/>
          <port xsi:type="esdl:OutPort" id="1a1b0f2b-18b9-47d8-a2c6-f429d5d7acf9" name="Out" connectedTo="2f65342c-f508-4b64-b175-55d9dbb9d0ec"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="9986fca5-fc6b-409a-a6d0-0c64e302d0ba">
          <port xsi:type="esdl:InPort" id="beb4dd8b-ba3c-475d-947c-bc8e2c15210b" name="In_Ph1" connectedTo="d8254743-7ff1-475a-87f3-995b44f8ff97"/>
          <port xsi:type="esdl:InPort" id="29c1678c-5c22-4068-876b-97436af468ae" name="In_Ph2" connectedTo="75396de5-1ec0-47b0-843e-fdab3dd78975"/>
          <port xsi:type="esdl:InPort" id="2f65342c-f508-4b64-b175-55d9dbb9d0ec" name="In_Ph3" connectedTo="1a1b0f2b-18b9-47d8-a2c6-f429d5d7acf9"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home0-1" id="123c7aba-6c26-4213-84fb-d48d33a8ae41">
          <port xsi:type="esdl:InPort" id="276bca73-9b03-4ddf-b93d-7248fd3ca260" name="In" connectedTo="acfa05a4-d7dc-4d1c-8ebb-f1fb368d4246"/>
          <port xsi:type="esdl:OutPort" id="3292d6a6-38b7-48e5-b3da-2148ff7e6537" name="OutPh1" connectedTo="408e516f-ff1d-4cd9-b8ef-f9788341e444"/>
          <port xsi:type="esdl:OutPort" id="44068fe8-a93c-4b10-b660-a44fe2c39a2e" name="OutPh2" connectedTo="2e416b2b-d5f9-4ab9-ac16-c21c3a67551a"/>
          <port xsi:type="esdl:OutPort" id="332b29ad-0588-4f3c-b553-268ceb308370" name="OutPh3" connectedTo="f02090db-e48f-4bf6-b744-0226bb0c1af2"/>
          <geometry xsi:type="esdl:Point" lat="12.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home0-2" id="aad1a484-7086-4a11-adca-61009b27e4bf">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="e1a33ca2-f34d-48b1-9738-3c5bf3d745a8" connectedTo="a071ec0d-d8e4-4985-9c51-5be2fd80b3a5" name="In"/>
          <port xsi:type="esdl:OutPort" id="b8bb700e-2899-4d69-900f-28a353b2b8ce" name="Out" connectedTo="3144ed68-a59c-47ab-9611-f00c601378f0"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="a0ac836e-9518-440b-9997-5e35f72625b7" connectedTo="5e0fe871-d1ca-49bc-9bb7-1fe57ebf2067" name="In"/>
          <port xsi:type="esdl:OutPort" id="e12f0511-5b51-4b26-ba26-cf80602959fc" name="Out" connectedTo="374c5eee-c02d-46e9-9c88-cce854e4b193"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="8ff61382-7deb-42e8-9b8f-a4fa9190095a" connectedTo="e712af93-c5e4-4eb7-82f5-24e50b8ec4f0" name="In"/>
          <port xsi:type="esdl:OutPort" id="a83ad9f6-1849-4a39-950e-b723c0184e2a" name="Out" connectedTo="22afb940-038f-40b2-9484-04b6d7dac293"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="3dede730-41bc-474b-a53a-d020be37a522">
          <port xsi:type="esdl:InPort" id="3144ed68-a59c-47ab-9611-f00c601378f0" name="In_Ph1" connectedTo="b8bb700e-2899-4d69-900f-28a353b2b8ce"/>
          <port xsi:type="esdl:InPort" id="374c5eee-c02d-46e9-9c88-cce854e4b193" name="In_Ph2" connectedTo="e12f0511-5b51-4b26-ba26-cf80602959fc"/>
          <port xsi:type="esdl:InPort" id="22afb940-038f-40b2-9484-04b6d7dac293" name="In_Ph3" connectedTo="a83ad9f6-1849-4a39-950e-b723c0184e2a"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home0-2" id="ebcb35f7-af2a-4ceb-bac5-1fb3d9f8996c">
          <port xsi:type="esdl:InPort" id="16bfad39-c37b-4265-bfcd-3025e83290f5" name="In" connectedTo="4ba8d249-eb5e-4f70-b4fc-650d55315caa"/>
          <port xsi:type="esdl:OutPort" id="a071ec0d-d8e4-4985-9c51-5be2fd80b3a5" name="OutPh1" connectedTo="e1a33ca2-f34d-48b1-9738-3c5bf3d745a8"/>
          <port xsi:type="esdl:OutPort" id="5e0fe871-d1ca-49bc-9bb7-1fe57ebf2067" name="OutPh2" connectedTo="a0ac836e-9518-440b-9997-5e35f72625b7"/>
          <port xsi:type="esdl:OutPort" id="e712af93-c5e4-4eb7-82f5-24e50b8ec4f0" name="OutPh3" connectedTo="8ff61382-7deb-42e8-9b8f-a4fa9190095a"/>
          <geometry xsi:type="esdl:Point" lat="12.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable1" length="6.0" assetType="lv_line" id="e4806b36-35ce-48df-9802-38b6bcd83153">
        <port xsi:type="esdl:InPort" id="ea583250-e067-47d3-8abc-79f4124ac528" name="In" connectedTo="9d02b99a-e188-45fd-9cba-30a8bb36590d"/>
        <port xsi:type="esdl:OutPort" id="2df015ae-65c0-4f75-9943-37527827a311" name="Out" connectedTo="f92a4d02-c1cf-46c6-879e-e7e134f53920"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="12.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint1" id="42b47db7-811a-4252-9716-0ee2b805f072">
        <port xsi:type="esdl:InPort" id="f92a4d02-c1cf-46c6-879e-e7e134f53920" name="In" connectedTo="2df015ae-65c0-4f75-9943-37527827a311"/>
        <port xsi:type="esdl:OutPort" id="c4cbf66c-7e27-4fec-80e7-0f0d1ca11b21" name="Out" connectedTo="6a295fa4-273a-4927-9315-770ddd75b9ea 1b6a8b84-ec9b-4546-9289-70dfba4cc026 ad08b3b3-8f45-47a9-856f-861e129c0e2e"/>
        <geometry xsi:type="esdl:Point" lat="18.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home1-1" length="1.0" assetType="lv_line_to_home" id="e7f8dc4b-d3e6-4c3e-ac86-1c15dced74c7">
        <port xsi:type="esdl:InPort" id="6a295fa4-273a-4927-9315-770ddd75b9ea" name="In" connectedTo="c4cbf66c-7e27-4fec-80e7-0f0d1ca11b21"/>
        <port xsi:type="esdl:OutPort" id="60da576c-76a1-4ad6-9013-53021a81ac8f" name="Out" connectedTo="b8df4cfa-833b-4385-98de-cf9e2c8386e6"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home1-2" length="1.0" assetType="lv_line_to_home" id="2603d7f0-d2ab-47a1-a645-e9414820213e">
        <port xsi:type="esdl:InPort" id="1b6a8b84-ec9b-4546-9289-70dfba4cc026" name="In" connectedTo="c4cbf66c-7e27-4fec-80e7-0f0d1ca11b21"/>
        <port xsi:type="esdl:OutPort" id="b88ec6e7-0ce4-48e4-a49e-d947e173c4e5" name="Out" connectedTo="55f54dbd-8c52-45b2-92e8-85daf8b8642a"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home1-1" id="3b291dce-496f-4102-9194-4458c6e5e120">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="285bee4d-ef12-4fba-85a1-32ed7c1cc7e2" connectedTo="e74d33b1-b756-481b-95ef-56247f6fd78b" name="In"/>
          <port xsi:type="esdl:OutPort" id="6a344e00-d842-4946-ad6f-795cfdeb05d1" name="Out" connectedTo="7efce353-4fe7-4334-adb6-6b0503e34f41"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="6ff20f83-063f-4e45-b472-70ccaa986bd2" connectedTo="b0e1cf55-505c-4423-b61e-00fa710092eb" name="In"/>
          <port xsi:type="esdl:OutPort" id="474e0047-3c02-4eff-8c92-a22acbf915dc" name="Out" connectedTo="3c7be13e-3fb6-4836-8c56-a79fbc3dd9bf"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="5987bf30-0876-452f-8a03-7df31bbce392" connectedTo="1067f5ee-9e41-4d02-b40e-f3ed411bec81" name="In"/>
          <port xsi:type="esdl:OutPort" id="d4a5af1d-4422-4f69-b60f-1fd78a5443ac" name="Out" connectedTo="1f822bc6-ff21-4f39-95d6-ab359e6d591e"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="98779c09-62ce-44ae-97d3-966a93333ce2">
          <port xsi:type="esdl:InPort" id="7efce353-4fe7-4334-adb6-6b0503e34f41" name="In_Ph1" connectedTo="6a344e00-d842-4946-ad6f-795cfdeb05d1"/>
          <port xsi:type="esdl:InPort" id="3c7be13e-3fb6-4836-8c56-a79fbc3dd9bf" name="In_Ph2" connectedTo="474e0047-3c02-4eff-8c92-a22acbf915dc"/>
          <port xsi:type="esdl:InPort" id="1f822bc6-ff21-4f39-95d6-ab359e6d591e" name="In_Ph3" connectedTo="d4a5af1d-4422-4f69-b60f-1fd78a5443ac"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home1-1" id="3ffd8275-9968-4e40-9e08-91cb2bd953a2">
          <port xsi:type="esdl:InPort" id="55f54dbd-8c52-45b2-92e8-85daf8b8642a" name="In" connectedTo="b88ec6e7-0ce4-48e4-a49e-d947e173c4e5"/>
          <port xsi:type="esdl:OutPort" id="e74d33b1-b756-481b-95ef-56247f6fd78b" name="OutPh1" connectedTo="285bee4d-ef12-4fba-85a1-32ed7c1cc7e2"/>
          <port xsi:type="esdl:OutPort" id="b0e1cf55-505c-4423-b61e-00fa710092eb" name="OutPh2" connectedTo="6ff20f83-063f-4e45-b472-70ccaa986bd2"/>
          <port xsi:type="esdl:OutPort" id="1067f5ee-9e41-4d02-b40e-f3ed411bec81" name="OutPh3" connectedTo="5987bf30-0876-452f-8a03-7df31bbce392"/>
          <geometry xsi:type="esdl:Point" lat="18.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home1-2" id="718507e8-4d10-44f0-96c3-6beaf457a8ea">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="0992cee8-84fd-469c-85fb-148ad5920725" connectedTo="86c5144c-9af2-458f-bac0-4bdb84e9e7c1" name="In"/>
          <port xsi:type="esdl:OutPort" id="beb8e2fe-8582-43f4-b19a-95bd18e16714" name="Out" connectedTo="7655f073-d1d2-4325-886b-79fc1dac09b6"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="bea28822-fef6-44c0-a741-8a0e6cb75ebd" connectedTo="1fc4ade9-b0eb-46f4-b9eb-52613f9901bb" name="In"/>
          <port xsi:type="esdl:OutPort" id="29708eec-1def-4da5-9503-c2c9a5752a47" name="Out" connectedTo="5e6eda75-1bb7-4c6e-8806-188735f2e4c3"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="619aed5e-ff4f-4d19-ac17-003af8517104" connectedTo="45de6c16-d4fa-425e-821b-b73c9aa1ba4a" name="In"/>
          <port xsi:type="esdl:OutPort" id="201767f6-84fb-4443-b836-3815a780d40a" name="Out" connectedTo="93d435e2-74cc-4d29-bd0c-1ac2c657a5ae"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="52b39e7f-d4a8-4cff-807e-a657c6ee4b80">
          <port xsi:type="esdl:InPort" id="7655f073-d1d2-4325-886b-79fc1dac09b6" name="In_Ph1" connectedTo="beb8e2fe-8582-43f4-b19a-95bd18e16714"/>
          <port xsi:type="esdl:InPort" id="5e6eda75-1bb7-4c6e-8806-188735f2e4c3" name="In_Ph2" connectedTo="29708eec-1def-4da5-9503-c2c9a5752a47"/>
          <port xsi:type="esdl:InPort" id="93d435e2-74cc-4d29-bd0c-1ac2c657a5ae" name="In_Ph3" connectedTo="201767f6-84fb-4443-b836-3815a780d40a"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home1-2" id="55621382-aea0-46ac-84fa-e646de1839aa">
          <port xsi:type="esdl:InPort" id="b8df4cfa-833b-4385-98de-cf9e2c8386e6" name="In" connectedTo="60da576c-76a1-4ad6-9013-53021a81ac8f"/>
          <port xsi:type="esdl:OutPort" id="86c5144c-9af2-458f-bac0-4bdb84e9e7c1" name="OutPh1" connectedTo="0992cee8-84fd-469c-85fb-148ad5920725"/>
          <port xsi:type="esdl:OutPort" id="1fc4ade9-b0eb-46f4-b9eb-52613f9901bb" name="OutPh2" connectedTo="bea28822-fef6-44c0-a741-8a0e6cb75ebd"/>
          <port xsi:type="esdl:OutPort" id="45de6c16-d4fa-425e-821b-b73c9aa1ba4a" name="OutPh3" connectedTo="619aed5e-ff4f-4d19-ac17-003af8517104"/>
          <geometry xsi:type="esdl:Point" lat="18.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable2" length="6.0" assetType="lv_line" id="056a5d9f-3948-426a-a9aa-5dd112a0e20b">
        <port xsi:type="esdl:InPort" id="ad08b3b3-8f45-47a9-856f-861e129c0e2e" name="In" connectedTo="c4cbf66c-7e27-4fec-80e7-0f0d1ca11b21"/>
        <port xsi:type="esdl:OutPort" id="17428034-f569-4c89-89ff-2d2457c51a63" name="Out" connectedTo="7d4c53a6-5646-4191-afed-72fdfd84a470"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint2" id="2108a9a8-e7ad-443c-8095-63b25d266782">
        <port xsi:type="esdl:InPort" id="7d4c53a6-5646-4191-afed-72fdfd84a470" name="In" connectedTo="17428034-f569-4c89-89ff-2d2457c51a63"/>
        <port xsi:type="esdl:OutPort" id="43196df8-a0c4-4e0e-8030-ce6b7826d31a" name="Out" connectedTo="c860cd4a-4c0f-4bcf-8523-568e11a6226f 96d13e35-2594-440a-8099-9ba46f49c28e b6aa79a8-205f-4f1c-9e17-3a3b498b3741"/>
        <geometry xsi:type="esdl:Point" lat="24.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home2-1" length="1.0" assetType="lv_line_to_home" id="1410d50b-ed34-462e-a674-da79d13d771c">
        <port xsi:type="esdl:InPort" id="c860cd4a-4c0f-4bcf-8523-568e11a6226f" name="In" connectedTo="43196df8-a0c4-4e0e-8030-ce6b7826d31a"/>
        <port xsi:type="esdl:OutPort" id="2385365c-d9bd-4160-ab02-617df44e11f6" name="Out" connectedTo="1365046c-1214-4c1b-b8d4-12cc63692ecf"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home2-2" length="1.0" assetType="lv_line_to_home" id="f5720a12-fecc-42d4-93c8-f24f22aa3d87">
        <port xsi:type="esdl:InPort" id="96d13e35-2594-440a-8099-9ba46f49c28e" name="In" connectedTo="43196df8-a0c4-4e0e-8030-ce6b7826d31a"/>
        <port xsi:type="esdl:OutPort" id="c9b88d42-e368-4c35-9c1d-fa92fc105d16" name="Out" connectedTo="f287d6be-fb80-44c5-b676-981fe66b2186"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home2-1" id="efabd40a-2bba-46a2-96af-3d563369dcf6">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="bfe2febb-a1dc-4609-86fe-ac343a4cd57d" connectedTo="943aca00-ad9a-4637-922e-2bb4a1dfeb1b" name="In"/>
          <port xsi:type="esdl:OutPort" id="b25b1455-9e00-43cd-acfa-e1308a98828d" name="Out" connectedTo="d54570ca-2819-4dad-94bf-973db1a8abda"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="96a27b81-9855-49c0-b35d-607110fcdb72" connectedTo="121fe319-d328-40d7-b44a-2b384eaa38a6" name="In"/>
          <port xsi:type="esdl:OutPort" id="9a5821b8-a580-48d6-8d18-bb58600a33be" name="Out" connectedTo="f9963542-90c5-4463-899e-6e11e0b26e35"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="9310fb67-90d9-4edf-9fe1-902a2a20f4a8" connectedTo="9096f41f-092f-4f84-8e2a-786966b263cd" name="In"/>
          <port xsi:type="esdl:OutPort" id="3f1d0c99-ae97-432a-998f-f24a3b506f13" name="Out" connectedTo="fb32d6d5-3043-4435-8096-2e585b3fcb8c"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="5ef964c9-1b52-4078-b131-b9c7aa75dcea">
          <port xsi:type="esdl:InPort" id="d54570ca-2819-4dad-94bf-973db1a8abda" name="In_Ph1" connectedTo="b25b1455-9e00-43cd-acfa-e1308a98828d"/>
          <port xsi:type="esdl:InPort" id="f9963542-90c5-4463-899e-6e11e0b26e35" name="In_Ph2" connectedTo="9a5821b8-a580-48d6-8d18-bb58600a33be"/>
          <port xsi:type="esdl:InPort" id="fb32d6d5-3043-4435-8096-2e585b3fcb8c" name="In_Ph3" connectedTo="3f1d0c99-ae97-432a-998f-f24a3b506f13"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home2-1" id="e4603581-4d4e-4b48-a885-c021d726b706">
          <port xsi:type="esdl:InPort" id="f287d6be-fb80-44c5-b676-981fe66b2186" name="In" connectedTo="c9b88d42-e368-4c35-9c1d-fa92fc105d16"/>
          <port xsi:type="esdl:OutPort" id="943aca00-ad9a-4637-922e-2bb4a1dfeb1b" name="OutPh1" connectedTo="bfe2febb-a1dc-4609-86fe-ac343a4cd57d"/>
          <port xsi:type="esdl:OutPort" id="121fe319-d328-40d7-b44a-2b384eaa38a6" name="OutPh2" connectedTo="96a27b81-9855-49c0-b35d-607110fcdb72"/>
          <port xsi:type="esdl:OutPort" id="9096f41f-092f-4f84-8e2a-786966b263cd" name="OutPh3" connectedTo="9310fb67-90d9-4edf-9fe1-902a2a20f4a8"/>
          <geometry xsi:type="esdl:Point" lat="24.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home2-2" id="7ca97348-d25f-4a9a-bc34-cd05ec543e8c">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="61524546-af80-4c92-94cc-d8b6cc342eaa" connectedTo="9572f1cc-b9eb-49e7-8e0b-b59c555d46d8" name="In"/>
          <port xsi:type="esdl:OutPort" id="5d10aa2f-a3e4-47eb-a69b-f2b58b5a45d5" name="Out" connectedTo="a1355966-e6bb-4a69-8ee0-1c39484750af"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="6751ea54-607f-4c7c-9c44-b8ef04034a91" connectedTo="271e579b-36f0-40cb-8414-1db97458425c" name="In"/>
          <port xsi:type="esdl:OutPort" id="2d94da79-98dd-40e9-88e7-8635b287c907" name="Out" connectedTo="c0e55cc4-5118-4948-8d11-ab229c938870"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="5b1ab99b-cffe-4625-a4e8-536dab12b0cd" connectedTo="ed4a734e-8410-4436-ab37-9ed69ae5b373" name="In"/>
          <port xsi:type="esdl:OutPort" id="a18576f3-e863-4625-9599-e43944f64c32" name="Out" connectedTo="7ab2427f-ff31-482a-b0df-2e738cb170a2"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="2e892be9-1366-430f-a259-0ee30862e9da">
          <port xsi:type="esdl:InPort" id="a1355966-e6bb-4a69-8ee0-1c39484750af" name="In_Ph1" connectedTo="5d10aa2f-a3e4-47eb-a69b-f2b58b5a45d5"/>
          <port xsi:type="esdl:InPort" id="c0e55cc4-5118-4948-8d11-ab229c938870" name="In_Ph2" connectedTo="2d94da79-98dd-40e9-88e7-8635b287c907"/>
          <port xsi:type="esdl:InPort" id="7ab2427f-ff31-482a-b0df-2e738cb170a2" name="In_Ph3" connectedTo="a18576f3-e863-4625-9599-e43944f64c32"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home2-2" id="bb051d30-1fa7-491f-825f-95d43a4720a5">
          <port xsi:type="esdl:InPort" id="1365046c-1214-4c1b-b8d4-12cc63692ecf" name="In" connectedTo="2385365c-d9bd-4160-ab02-617df44e11f6"/>
          <port xsi:type="esdl:OutPort" id="9572f1cc-b9eb-49e7-8e0b-b59c555d46d8" name="OutPh1" connectedTo="61524546-af80-4c92-94cc-d8b6cc342eaa"/>
          <port xsi:type="esdl:OutPort" id="271e579b-36f0-40cb-8414-1db97458425c" name="OutPh2" connectedTo="6751ea54-607f-4c7c-9c44-b8ef04034a91"/>
          <port xsi:type="esdl:OutPort" id="ed4a734e-8410-4436-ab37-9ed69ae5b373" name="OutPh3" connectedTo="5b1ab99b-cffe-4625-a4e8-536dab12b0cd"/>
          <geometry xsi:type="esdl:Point" lat="24.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable3" length="6.0" assetType="lv_line" id="40fa7d8a-36fd-43dd-a161-15b79fefecff">
        <port xsi:type="esdl:InPort" id="b6aa79a8-205f-4f1c-9e17-3a3b498b3741" name="In" connectedTo="43196df8-a0c4-4e0e-8030-ce6b7826d31a"/>
        <port xsi:type="esdl:OutPort" id="3cfaa8d1-746e-476f-93e1-d1de7d422554" name="Out" connectedTo="8d53234a-161b-4501-858f-d69af9f4eb0d"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint3" id="55b3b4e8-79f2-4396-bd38-ba3c53e5f4ce">
        <port xsi:type="esdl:InPort" id="8d53234a-161b-4501-858f-d69af9f4eb0d" name="In" connectedTo="3cfaa8d1-746e-476f-93e1-d1de7d422554"/>
        <port xsi:type="esdl:OutPort" id="2f3574cd-d94b-40bc-8e1c-050e4d6e2df5" name="Out" connectedTo="3b5a1e8b-4917-4357-a11a-3c30935590c3 ed4c2a8f-d31c-43ad-b958-20b1241ca1aa 577b9ec2-d121-4e79-9657-eba471849361"/>
        <geometry xsi:type="esdl:Point" lat="30.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home3-1" length="1.0" assetType="lv_line_to_home" id="b93c5d14-c4eb-4eb9-87c8-abd8ab96e12d">
        <port xsi:type="esdl:InPort" id="3b5a1e8b-4917-4357-a11a-3c30935590c3" name="In" connectedTo="2f3574cd-d94b-40bc-8e1c-050e4d6e2df5"/>
        <port xsi:type="esdl:OutPort" id="1885b5e2-7cea-49db-9949-a19dd6cca159" name="Out" connectedTo="1f674034-60ce-4d02-808a-a8f13ec59d4c"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home3-2" length="1.0" assetType="lv_line_to_home" id="e90d5e29-7788-416e-a572-9a6bad0ee5c6">
        <port xsi:type="esdl:InPort" id="ed4c2a8f-d31c-43ad-b958-20b1241ca1aa" name="In" connectedTo="2f3574cd-d94b-40bc-8e1c-050e4d6e2df5"/>
        <port xsi:type="esdl:OutPort" id="60e13497-9feb-4217-9b39-c3ea9b71a987" name="Out" connectedTo="85b46fab-2174-44e1-9448-4c264760d7dd"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home3-1" id="84d6761e-3ea8-4b2a-8c75-7ec930683b14">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="85de9771-1f05-419d-8f09-9ca44fa405ed" connectedTo="db33c905-1224-4146-b481-410176171a7c" name="In"/>
          <port xsi:type="esdl:OutPort" id="f70202b1-65b3-4e49-8376-1e98b11409eb" name="Out" connectedTo="945ea8b9-d386-42d4-81c7-64e89e9a1784"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="f6f8ffb4-ea5c-488c-acf0-2c7a9267af5e" connectedTo="5cac4395-7fd8-49d5-8f36-bf880ace65e1" name="In"/>
          <port xsi:type="esdl:OutPort" id="8810cc85-95b0-454e-9e75-81a4b2595c96" name="Out" connectedTo="4c23b686-6136-4839-b7b7-23cdbb4a233e"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="a08acc9a-e817-423c-abfb-8bb91a4d90ba" connectedTo="135c2589-add2-43ce-836a-e78b42e708af" name="In"/>
          <port xsi:type="esdl:OutPort" id="5586101c-6c13-41e8-b9ad-fd3e6409f8ba" name="Out" connectedTo="2ac4944e-9cb4-4b0d-97fd-0369fbefc06d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="4ed6a28a-6031-4bfe-a3b4-d6ad6ab908b1">
          <port xsi:type="esdl:InPort" id="945ea8b9-d386-42d4-81c7-64e89e9a1784" name="In_Ph1" connectedTo="f70202b1-65b3-4e49-8376-1e98b11409eb"/>
          <port xsi:type="esdl:InPort" id="4c23b686-6136-4839-b7b7-23cdbb4a233e" name="In_Ph2" connectedTo="8810cc85-95b0-454e-9e75-81a4b2595c96"/>
          <port xsi:type="esdl:InPort" id="2ac4944e-9cb4-4b0d-97fd-0369fbefc06d" name="In_Ph3" connectedTo="5586101c-6c13-41e8-b9ad-fd3e6409f8ba"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home3-1" id="8774ac0c-2c7a-4e54-8298-f76d180dd308">
          <port xsi:type="esdl:InPort" id="85b46fab-2174-44e1-9448-4c264760d7dd" name="In" connectedTo="60e13497-9feb-4217-9b39-c3ea9b71a987"/>
          <port xsi:type="esdl:OutPort" id="db33c905-1224-4146-b481-410176171a7c" name="OutPh1" connectedTo="85de9771-1f05-419d-8f09-9ca44fa405ed"/>
          <port xsi:type="esdl:OutPort" id="5cac4395-7fd8-49d5-8f36-bf880ace65e1" name="OutPh2" connectedTo="f6f8ffb4-ea5c-488c-acf0-2c7a9267af5e"/>
          <port xsi:type="esdl:OutPort" id="135c2589-add2-43ce-836a-e78b42e708af" name="OutPh3" connectedTo="a08acc9a-e817-423c-abfb-8bb91a4d90ba"/>
          <geometry xsi:type="esdl:Point" lat="30.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home3-2" id="0d261484-f4ed-4144-9144-58fdf968a2f5">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="de065ef4-9766-4e7a-b7f5-2ca135ff9ac0" connectedTo="f6300b25-2f43-4fc9-a545-a6d57fe8685e" name="In"/>
          <port xsi:type="esdl:OutPort" id="1ded5c9c-3de0-4957-8311-88e6385eb884" name="Out" connectedTo="909d5bb0-7c07-44b5-8fa9-cf8c3dc4f3c7"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="284733fa-fe29-428d-8d3a-3f16e1b9ffc5" connectedTo="5e8c3361-6b4a-4faf-8991-a46e2b965b70" name="In"/>
          <port xsi:type="esdl:OutPort" id="521a628b-a787-4d7b-897b-c39ed0f4f667" name="Out" connectedTo="e6e9f630-3c5c-45f1-bd56-427387dd8134"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="547b8641-9e3c-4647-b75a-64a233ae06c6" connectedTo="7ae85196-18d2-42b7-8b46-e8aa1a42021e" name="In"/>
          <port xsi:type="esdl:OutPort" id="9a2ecc16-bc6d-4ed2-8a5e-5b37ed8fc2fe" name="Out" connectedTo="57b7577b-3c7b-453a-af95-f5f00cee1d42"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="6da1f1a2-4444-4fcf-aeb1-2f3751e29646">
          <port xsi:type="esdl:InPort" id="909d5bb0-7c07-44b5-8fa9-cf8c3dc4f3c7" name="In_Ph1" connectedTo="1ded5c9c-3de0-4957-8311-88e6385eb884"/>
          <port xsi:type="esdl:InPort" id="e6e9f630-3c5c-45f1-bd56-427387dd8134" name="In_Ph2" connectedTo="521a628b-a787-4d7b-897b-c39ed0f4f667"/>
          <port xsi:type="esdl:InPort" id="57b7577b-3c7b-453a-af95-f5f00cee1d42" name="In_Ph3" connectedTo="9a2ecc16-bc6d-4ed2-8a5e-5b37ed8fc2fe"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home3-2" id="03c30081-51a5-43bf-82c2-606d44a8ec19">
          <port xsi:type="esdl:InPort" id="1f674034-60ce-4d02-808a-a8f13ec59d4c" name="In" connectedTo="1885b5e2-7cea-49db-9949-a19dd6cca159"/>
          <port xsi:type="esdl:OutPort" id="f6300b25-2f43-4fc9-a545-a6d57fe8685e" name="OutPh1" connectedTo="de065ef4-9766-4e7a-b7f5-2ca135ff9ac0"/>
          <port xsi:type="esdl:OutPort" id="5e8c3361-6b4a-4faf-8991-a46e2b965b70" name="OutPh2" connectedTo="284733fa-fe29-428d-8d3a-3f16e1b9ffc5"/>
          <port xsi:type="esdl:OutPort" id="7ae85196-18d2-42b7-8b46-e8aa1a42021e" name="OutPh3" connectedTo="547b8641-9e3c-4647-b75a-64a233ae06c6"/>
          <geometry xsi:type="esdl:Point" lat="30.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable4" length="6.0" assetType="lv_line" id="bb7c6daf-19c9-4146-b403-0ebd3028d5df">
        <port xsi:type="esdl:InPort" id="577b9ec2-d121-4e79-9657-eba471849361" name="In" connectedTo="2f3574cd-d94b-40bc-8e1c-050e4d6e2df5"/>
        <port xsi:type="esdl:OutPort" id="397628df-3809-4d4b-a972-cd19b610b2b5" name="Out" connectedTo="40009832-3dd5-4743-9fab-6fa4d7b26286"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint4" id="e5e52f46-d8a6-44d9-904d-3830ff0103b6">
        <port xsi:type="esdl:InPort" id="40009832-3dd5-4743-9fab-6fa4d7b26286" name="In" connectedTo="397628df-3809-4d4b-a972-cd19b610b2b5"/>
        <port xsi:type="esdl:OutPort" id="39466dd8-e2a0-4612-b7de-40066938d7e4" name="Out" connectedTo="0f7289d3-177d-4208-af07-cd7c40b2b49d 13e5d160-aaa2-46b6-a7c0-3ca16be662d1 ce0dd915-f4c8-4097-a218-4b0073ad8ed6"/>
        <geometry xsi:type="esdl:Point" lat="36.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home4-1" length="1.0" assetType="lv_line_to_home" id="2c2b5c7e-7312-4a92-89c5-2a33662afb89">
        <port xsi:type="esdl:InPort" id="0f7289d3-177d-4208-af07-cd7c40b2b49d" name="In" connectedTo="39466dd8-e2a0-4612-b7de-40066938d7e4"/>
        <port xsi:type="esdl:OutPort" id="fb1703f2-f093-4bac-9ef5-1e9cc8194dc6" name="Out" connectedTo="28b871c5-640a-4163-bed1-2bc2ffa8c04b"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home4-2" length="1.0" assetType="lv_line_to_home" id="1b384e2c-c294-4139-9561-ef9d49f1e721">
        <port xsi:type="esdl:InPort" id="13e5d160-aaa2-46b6-a7c0-3ca16be662d1" name="In" connectedTo="39466dd8-e2a0-4612-b7de-40066938d7e4"/>
        <port xsi:type="esdl:OutPort" id="c44d38e1-6cbc-48cf-850f-629cf5cf4bd6" name="Out" connectedTo="1247cd13-d924-449c-9e68-1d543a3f3ed1"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home4-1" id="dc37a0f3-94fd-4f36-92a1-430100d99abb">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="1a984d4a-49de-4bc4-86a9-6a4e4c2cef67" connectedTo="de982fb6-336b-498d-b738-0e527a71869f" name="In"/>
          <port xsi:type="esdl:OutPort" id="1d308b6f-4b60-4682-9e2f-39d317032998" name="Out" connectedTo="d4a96663-74c0-44c6-a1e2-025e16cc3d17"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="32cdba69-fcf5-45fd-88d8-5c26e1e38780" connectedTo="43a65203-2fcc-4f6d-a722-01a76e77cba9" name="In"/>
          <port xsi:type="esdl:OutPort" id="da10f1a9-dc98-4752-b02f-0671d5592877" name="Out" connectedTo="b2ffd10f-ceb4-4275-81bc-73910bab4db3"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="654f8c97-46e3-44c3-a4da-4a1976eacf93" connectedTo="c8d6d357-433e-4b3b-be17-a5bb68680a1a" name="In"/>
          <port xsi:type="esdl:OutPort" id="a0860712-5a8b-4bad-93db-098977d71ba3" name="Out" connectedTo="85062a91-2365-453f-a4aa-c1bd135c5286"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="ee1a0b51-bac2-433c-9dfd-ed0e239ba64d">
          <port xsi:type="esdl:InPort" id="d4a96663-74c0-44c6-a1e2-025e16cc3d17" name="In_Ph1" connectedTo="1d308b6f-4b60-4682-9e2f-39d317032998"/>
          <port xsi:type="esdl:InPort" id="b2ffd10f-ceb4-4275-81bc-73910bab4db3" name="In_Ph2" connectedTo="da10f1a9-dc98-4752-b02f-0671d5592877"/>
          <port xsi:type="esdl:InPort" id="85062a91-2365-453f-a4aa-c1bd135c5286" name="In_Ph3" connectedTo="a0860712-5a8b-4bad-93db-098977d71ba3"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home4-1" id="0db51559-76c6-4a23-a3ee-766f0e73a919">
          <port xsi:type="esdl:InPort" id="1247cd13-d924-449c-9e68-1d543a3f3ed1" name="In" connectedTo="c44d38e1-6cbc-48cf-850f-629cf5cf4bd6"/>
          <port xsi:type="esdl:OutPort" id="de982fb6-336b-498d-b738-0e527a71869f" name="OutPh1" connectedTo="1a984d4a-49de-4bc4-86a9-6a4e4c2cef67"/>
          <port xsi:type="esdl:OutPort" id="43a65203-2fcc-4f6d-a722-01a76e77cba9" name="OutPh2" connectedTo="32cdba69-fcf5-45fd-88d8-5c26e1e38780"/>
          <port xsi:type="esdl:OutPort" id="c8d6d357-433e-4b3b-be17-a5bb68680a1a" name="OutPh3" connectedTo="654f8c97-46e3-44c3-a4da-4a1976eacf93"/>
          <geometry xsi:type="esdl:Point" lat="36.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home4-2" id="d27111a7-bc4b-4206-b796-22ec48ddc7b1">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="8e7f423e-20e9-4c55-a15d-eed5e00ebef6" connectedTo="042b7a52-4620-4516-a862-fb55c0142577" name="In"/>
          <port xsi:type="esdl:OutPort" id="3e70a62b-7a0a-45a8-9bf6-6d1472636087" name="Out" connectedTo="ec2f8a30-e5b4-48d1-87ac-648418945ac3"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="47c8bff8-29c8-4816-92ba-8e957bb60de5" connectedTo="54b8e474-45ee-4bfc-a2c8-449fa49990c6" name="In"/>
          <port xsi:type="esdl:OutPort" id="31686a6a-a224-4649-bdb9-c9d2bd8779b0" name="Out" connectedTo="0d02f599-1be1-4075-b8b8-4909e11419a3"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="d577519b-bf4f-4a62-a782-26f65fc0b62b" connectedTo="8048799f-d964-4bb1-9f43-21d347c92872" name="In"/>
          <port xsi:type="esdl:OutPort" id="a62eeb45-6c10-46f2-a675-ce36281cbc11" name="Out" connectedTo="9d893585-b911-46cd-a79f-d858a2b9804b"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="d819287f-d1ee-4f23-a7f1-e983bc273483">
          <port xsi:type="esdl:InPort" id="ec2f8a30-e5b4-48d1-87ac-648418945ac3" name="In_Ph1" connectedTo="3e70a62b-7a0a-45a8-9bf6-6d1472636087"/>
          <port xsi:type="esdl:InPort" id="0d02f599-1be1-4075-b8b8-4909e11419a3" name="In_Ph2" connectedTo="31686a6a-a224-4649-bdb9-c9d2bd8779b0"/>
          <port xsi:type="esdl:InPort" id="9d893585-b911-46cd-a79f-d858a2b9804b" name="In_Ph3" connectedTo="a62eeb45-6c10-46f2-a675-ce36281cbc11"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home4-2" id="be6f5c28-9dcb-44ea-8199-9db70e114180">
          <port xsi:type="esdl:InPort" id="28b871c5-640a-4163-bed1-2bc2ffa8c04b" name="In" connectedTo="fb1703f2-f093-4bac-9ef5-1e9cc8194dc6"/>
          <port xsi:type="esdl:OutPort" id="042b7a52-4620-4516-a862-fb55c0142577" name="OutPh1" connectedTo="8e7f423e-20e9-4c55-a15d-eed5e00ebef6"/>
          <port xsi:type="esdl:OutPort" id="54b8e474-45ee-4bfc-a2c8-449fa49990c6" name="OutPh2" connectedTo="47c8bff8-29c8-4816-92ba-8e957bb60de5"/>
          <port xsi:type="esdl:OutPort" id="8048799f-d964-4bb1-9f43-21d347c92872" name="OutPh3" connectedTo="d577519b-bf4f-4a62-a782-26f65fc0b62b"/>
          <geometry xsi:type="esdl:Point" lat="36.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable5" length="6.0" assetType="lv_line" id="60485893-a8ee-4697-82ef-4eae958b004b">
        <port xsi:type="esdl:InPort" id="ce0dd915-f4c8-4097-a218-4b0073ad8ed6" name="In" connectedTo="39466dd8-e2a0-4612-b7de-40066938d7e4"/>
        <port xsi:type="esdl:OutPort" id="fcdd6dd5-1dfe-4d4a-9ff0-1187acea75d3" name="Out" connectedTo="31b15de5-1c5f-4289-87fa-bfbeeacd9ec4"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint5" id="7f61cf1a-cd3f-4970-8ccc-06a89a5a2277">
        <port xsi:type="esdl:InPort" id="31b15de5-1c5f-4289-87fa-bfbeeacd9ec4" name="In" connectedTo="fcdd6dd5-1dfe-4d4a-9ff0-1187acea75d3"/>
        <port xsi:type="esdl:OutPort" id="9715fb5f-b093-457b-9df1-afc3e2d98b20" name="Out" connectedTo="455b0bc9-1fdf-4001-93a5-a75d2f659b36 2b388fbd-52b0-4396-a597-3addcbecdcca 2b8b6a89-2b18-4b2d-b4c9-a07ce84471e4"/>
        <geometry xsi:type="esdl:Point" lat="42.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home5-1" length="1.0" assetType="lv_line_to_home" id="199090a7-2bd8-415b-b850-7661bb38ce1a">
        <port xsi:type="esdl:InPort" id="455b0bc9-1fdf-4001-93a5-a75d2f659b36" name="In" connectedTo="9715fb5f-b093-457b-9df1-afc3e2d98b20"/>
        <port xsi:type="esdl:OutPort" id="100618e8-98fb-4fe3-861f-ab171473e18d" name="Out" connectedTo="65feefbc-3f86-4d8f-acdb-358e2cb034ca"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home5-2" length="1.0" assetType="lv_line_to_home" id="5e113ced-4017-4b05-9096-ec104beebb33">
        <port xsi:type="esdl:InPort" id="2b388fbd-52b0-4396-a597-3addcbecdcca" name="In" connectedTo="9715fb5f-b093-457b-9df1-afc3e2d98b20"/>
        <port xsi:type="esdl:OutPort" id="8601c9f2-63cc-4a2a-aee0-21c342fe79b1" name="Out" connectedTo="b7d96e48-6ab3-4afa-8224-d82d18cc0579"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home5-1" id="4a36319d-2ceb-46a1-9a9e-8dae11363d71">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="4b9dd06a-4632-470e-af95-76af91732c78" connectedTo="6563cf48-df72-451c-a28f-1e5ddafcdca3" name="In"/>
          <port xsi:type="esdl:OutPort" id="840c3e18-87ba-48b1-8c83-53c0d339980f" name="Out" connectedTo="0be09a87-8f93-4bb3-a2ff-f6bed3809282"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="76f3922e-3298-454f-8b6f-aefedf6ae7a7" connectedTo="86333206-4bb4-41c9-a1ef-62ed78ffbec5" name="In"/>
          <port xsi:type="esdl:OutPort" id="e020413a-59a9-428f-ab60-ee88c10e1dcf" name="Out" connectedTo="53287424-9d7c-400f-a9f3-e24ef0968add"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="0d1c8b97-6c50-482e-a41e-4e48aebcb52b" connectedTo="ab40e219-ec23-4162-8f25-443ad6af427c" name="In"/>
          <port xsi:type="esdl:OutPort" id="2df09203-6b7a-4163-83ae-fb48a451c9ae" name="Out" connectedTo="07ff6a23-615b-4f18-af41-37905e7b8005"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="62099eaa-ef20-45cc-aa53-3da6975b987a">
          <port xsi:type="esdl:InPort" id="0be09a87-8f93-4bb3-a2ff-f6bed3809282" name="In_Ph1" connectedTo="840c3e18-87ba-48b1-8c83-53c0d339980f"/>
          <port xsi:type="esdl:InPort" id="53287424-9d7c-400f-a9f3-e24ef0968add" name="In_Ph2" connectedTo="e020413a-59a9-428f-ab60-ee88c10e1dcf"/>
          <port xsi:type="esdl:InPort" id="07ff6a23-615b-4f18-af41-37905e7b8005" name="In_Ph3" connectedTo="2df09203-6b7a-4163-83ae-fb48a451c9ae"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home5-1" id="9a33f689-4ace-4e5f-85f3-b2f45719b525">
          <port xsi:type="esdl:InPort" id="b7d96e48-6ab3-4afa-8224-d82d18cc0579" name="In" connectedTo="8601c9f2-63cc-4a2a-aee0-21c342fe79b1"/>
          <port xsi:type="esdl:OutPort" id="6563cf48-df72-451c-a28f-1e5ddafcdca3" name="OutPh1" connectedTo="4b9dd06a-4632-470e-af95-76af91732c78"/>
          <port xsi:type="esdl:OutPort" id="86333206-4bb4-41c9-a1ef-62ed78ffbec5" name="OutPh2" connectedTo="76f3922e-3298-454f-8b6f-aefedf6ae7a7"/>
          <port xsi:type="esdl:OutPort" id="ab40e219-ec23-4162-8f25-443ad6af427c" name="OutPh3" connectedTo="0d1c8b97-6c50-482e-a41e-4e48aebcb52b"/>
          <geometry xsi:type="esdl:Point" lat="42.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home5-2" id="dec9fb14-ca5c-4974-b70e-46dff7fa0a99">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="4eddad04-52fd-4ce5-908c-6d409ee5e9d5" connectedTo="451c7e2c-908e-44a6-a90b-e787decb789c" name="In"/>
          <port xsi:type="esdl:OutPort" id="3956c14c-e0e6-4804-a085-c94e1a214c08" name="Out" connectedTo="79cd864e-2bd0-4087-be93-71ee0c61a638"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="ee5d0080-a839-4ae0-b2f3-e882e8421cb9" connectedTo="bb863132-70eb-4900-9df6-cdb2bc18e308" name="In"/>
          <port xsi:type="esdl:OutPort" id="b98d6acb-d83e-431e-a0bb-3095faf4bec8" name="Out" connectedTo="b516f62c-0b40-438d-a8be-96ab0a2aa356"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="70e9c9dd-43c0-4536-9026-657d4d636a49" connectedTo="6555056c-114c-4b24-a21b-5562e9a2a363" name="In"/>
          <port xsi:type="esdl:OutPort" id="34eca5cc-a8f9-4b52-bcf6-bc94ea0c3e42" name="Out" connectedTo="7d0b3ed5-8ba5-4895-893d-5fd4916f5e7d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="792f9596-3f64-4860-8507-1ac11eaa482b">
          <port xsi:type="esdl:InPort" id="79cd864e-2bd0-4087-be93-71ee0c61a638" name="In_Ph1" connectedTo="3956c14c-e0e6-4804-a085-c94e1a214c08"/>
          <port xsi:type="esdl:InPort" id="b516f62c-0b40-438d-a8be-96ab0a2aa356" name="In_Ph2" connectedTo="b98d6acb-d83e-431e-a0bb-3095faf4bec8"/>
          <port xsi:type="esdl:InPort" id="7d0b3ed5-8ba5-4895-893d-5fd4916f5e7d" name="In_Ph3" connectedTo="34eca5cc-a8f9-4b52-bcf6-bc94ea0c3e42"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home5-2" id="525d14d0-2399-45e3-a478-3781262b9e50">
          <port xsi:type="esdl:InPort" id="65feefbc-3f86-4d8f-acdb-358e2cb034ca" name="In" connectedTo="100618e8-98fb-4fe3-861f-ab171473e18d"/>
          <port xsi:type="esdl:OutPort" id="451c7e2c-908e-44a6-a90b-e787decb789c" name="OutPh1" connectedTo="4eddad04-52fd-4ce5-908c-6d409ee5e9d5"/>
          <port xsi:type="esdl:OutPort" id="bb863132-70eb-4900-9df6-cdb2bc18e308" name="OutPh2" connectedTo="ee5d0080-a839-4ae0-b2f3-e882e8421cb9"/>
          <port xsi:type="esdl:OutPort" id="6555056c-114c-4b24-a21b-5562e9a2a363" name="OutPh3" connectedTo="70e9c9dd-43c0-4536-9026-657d4d636a49"/>
          <geometry xsi:type="esdl:Point" lat="42.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable6" length="6.0" assetType="lv_line" id="5ab8aac0-2939-4893-b243-9afdb6199aea">
        <port xsi:type="esdl:InPort" id="2b8b6a89-2b18-4b2d-b4c9-a07ce84471e4" name="In" connectedTo="9715fb5f-b093-457b-9df1-afc3e2d98b20"/>
        <port xsi:type="esdl:OutPort" id="896df574-94b4-4542-94aa-2d2eed2892c0" name="Out" connectedTo="12e70f2b-6f77-464d-8680-f5807790bf53"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="48.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint6" id="743c972e-e13d-4372-8d54-27a699d3d1a7">
        <port xsi:type="esdl:InPort" id="12e70f2b-6f77-464d-8680-f5807790bf53" name="In" connectedTo="896df574-94b4-4542-94aa-2d2eed2892c0"/>
        <port xsi:type="esdl:OutPort" id="6bbd5a9c-8081-4d4a-a3e7-18cee63dc72f" name="Out" connectedTo="ffb9031d-bdb5-498c-90ca-47809ccb0c7e 0da9fcce-3950-4a37-9604-5756f9948b9c 74cc0811-f54e-4461-9bf2-4e50e7382f5d"/>
        <geometry xsi:type="esdl:Point" lat="48.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home6-1" length="1.0" assetType="lv_line_to_home" id="afd7e8ac-ca1a-4096-a738-1c940e8c2753">
        <port xsi:type="esdl:InPort" id="ffb9031d-bdb5-498c-90ca-47809ccb0c7e" name="In" connectedTo="6bbd5a9c-8081-4d4a-a3e7-18cee63dc72f"/>
        <port xsi:type="esdl:OutPort" id="cd43a515-93c4-4566-ada0-9f4f2b3ff66a" name="Out" connectedTo="2654a539-1011-40e0-b98a-e4672da612a0"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="48.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="48.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home6-2" length="1.0" assetType="lv_line_to_home" id="f5b6234a-e541-4018-b79d-1d8b0dccba8b">
        <port xsi:type="esdl:InPort" id="0da9fcce-3950-4a37-9604-5756f9948b9c" name="In" connectedTo="6bbd5a9c-8081-4d4a-a3e7-18cee63dc72f"/>
        <port xsi:type="esdl:OutPort" id="48325aa8-ff6c-49b1-96c3-7d9bfce563b5" name="Out" connectedTo="2d670140-3871-4d7b-8629-d20ec9699789"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="48.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="48.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home6-1" id="a1c1aa38-6e10-4a9a-a23f-5e1940375ac1">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="3ede486a-2540-4a03-b124-4ca816f09fad" connectedTo="77f3bfe1-ecc4-43bb-a35a-bfef243612a2" name="In"/>
          <port xsi:type="esdl:OutPort" id="86e69182-1745-4af8-8c84-e605898cf488" name="Out" connectedTo="70af5813-de61-4f56-9cc9-8cb54fa21eb2"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="c887007c-e8e9-49be-aed9-f0f5143da764" connectedTo="9d393bcc-d6e3-49cd-af1f-9816fba28dd2" name="In"/>
          <port xsi:type="esdl:OutPort" id="ac0b0fe7-c4cc-450a-a2dd-477cfcc6628f" name="Out" connectedTo="0243ff4a-a260-4345-b34d-858bdfbfb066"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="4ba22461-3995-49cc-8664-4c857dabcb8e" connectedTo="47b750ec-f78f-4a05-9bc8-af2c5be6abfa" name="In"/>
          <port xsi:type="esdl:OutPort" id="d2bb10c4-40d3-4b75-a920-30872cf23e90" name="Out" connectedTo="320c9fac-c80a-45ae-b0d0-fc1907770144"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="e351bc0c-bad6-4272-a4d6-089266d5d0f4">
          <port xsi:type="esdl:InPort" id="70af5813-de61-4f56-9cc9-8cb54fa21eb2" name="In_Ph1" connectedTo="86e69182-1745-4af8-8c84-e605898cf488"/>
          <port xsi:type="esdl:InPort" id="0243ff4a-a260-4345-b34d-858bdfbfb066" name="In_Ph2" connectedTo="ac0b0fe7-c4cc-450a-a2dd-477cfcc6628f"/>
          <port xsi:type="esdl:InPort" id="320c9fac-c80a-45ae-b0d0-fc1907770144" name="In_Ph3" connectedTo="d2bb10c4-40d3-4b75-a920-30872cf23e90"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home6-1" id="65f0c4a0-a48c-4101-8caa-321d383ded91">
          <port xsi:type="esdl:InPort" id="2d670140-3871-4d7b-8629-d20ec9699789" name="In" connectedTo="48325aa8-ff6c-49b1-96c3-7d9bfce563b5"/>
          <port xsi:type="esdl:OutPort" id="77f3bfe1-ecc4-43bb-a35a-bfef243612a2" name="OutPh1" connectedTo="3ede486a-2540-4a03-b124-4ca816f09fad"/>
          <port xsi:type="esdl:OutPort" id="9d393bcc-d6e3-49cd-af1f-9816fba28dd2" name="OutPh2" connectedTo="c887007c-e8e9-49be-aed9-f0f5143da764"/>
          <port xsi:type="esdl:OutPort" id="47b750ec-f78f-4a05-9bc8-af2c5be6abfa" name="OutPh3" connectedTo="4ba22461-3995-49cc-8664-4c857dabcb8e"/>
          <geometry xsi:type="esdl:Point" lat="48.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home6-2" id="740d35a6-05e6-4f3a-9f7d-6bf37f416f91">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="9dc56d54-79be-46e5-bd63-13d6069d55a1" connectedTo="7efa77b9-d6e1-48c2-b21d-487eaa0b1011" name="In"/>
          <port xsi:type="esdl:OutPort" id="7f7a9d29-fc92-4774-8412-03a700904bbf" name="Out" connectedTo="6a51290a-0d13-48d1-b999-c4fc1464afa5"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="1f1eacfb-5e2b-4615-8f95-749e6dec7ad7" connectedTo="86978c61-2b97-4ab5-89ca-3741479e533c" name="In"/>
          <port xsi:type="esdl:OutPort" id="07362459-caa3-421e-a99c-ca71ed5d7221" name="Out" connectedTo="7c3eac1b-452d-4425-9bf2-c255c2c4f43c"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="c23272ef-dfd8-4326-89ea-0165ae2db294" connectedTo="ed55da81-acb9-4d25-b4bc-6d2ce0248b48" name="In"/>
          <port xsi:type="esdl:OutPort" id="e0ebf614-dc8c-49ea-a02f-a92b3c4ed662" name="Out" connectedTo="795399d7-fdb2-4fbb-bdf6-b22a5b2d29e2"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="47ec3440-417a-4431-9f5f-df2480c2bc3b">
          <port xsi:type="esdl:InPort" id="6a51290a-0d13-48d1-b999-c4fc1464afa5" name="In_Ph1" connectedTo="7f7a9d29-fc92-4774-8412-03a700904bbf"/>
          <port xsi:type="esdl:InPort" id="7c3eac1b-452d-4425-9bf2-c255c2c4f43c" name="In_Ph2" connectedTo="07362459-caa3-421e-a99c-ca71ed5d7221"/>
          <port xsi:type="esdl:InPort" id="795399d7-fdb2-4fbb-bdf6-b22a5b2d29e2" name="In_Ph3" connectedTo="e0ebf614-dc8c-49ea-a02f-a92b3c4ed662"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home6-2" id="7765c615-04f0-4fbe-8e5c-23853b4173d7">
          <port xsi:type="esdl:InPort" id="2654a539-1011-40e0-b98a-e4672da612a0" name="In" connectedTo="cd43a515-93c4-4566-ada0-9f4f2b3ff66a"/>
          <port xsi:type="esdl:OutPort" id="7efa77b9-d6e1-48c2-b21d-487eaa0b1011" name="OutPh1" connectedTo="9dc56d54-79be-46e5-bd63-13d6069d55a1"/>
          <port xsi:type="esdl:OutPort" id="86978c61-2b97-4ab5-89ca-3741479e533c" name="OutPh2" connectedTo="1f1eacfb-5e2b-4615-8f95-749e6dec7ad7"/>
          <port xsi:type="esdl:OutPort" id="ed55da81-acb9-4d25-b4bc-6d2ce0248b48" name="OutPh3" connectedTo="c23272ef-dfd8-4326-89ea-0165ae2db294"/>
          <geometry xsi:type="esdl:Point" lat="48.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable7" length="6.0" assetType="lv_line" id="91b926bb-c526-4786-992f-04266f0dc655">
        <port xsi:type="esdl:InPort" id="74cc0811-f54e-4461-9bf2-4e50e7382f5d" name="In" connectedTo="6bbd5a9c-8081-4d4a-a3e7-18cee63dc72f"/>
        <port xsi:type="esdl:OutPort" id="79348c3e-87d7-4fea-9e89-d8f625889e8f" name="Out" connectedTo="0dbad30b-f590-4c0f-8f5d-941ff59a4043"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="48.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="54.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint7" id="ce1df9df-8560-40a8-8845-95b07e1dbb8b">
        <port xsi:type="esdl:InPort" id="0dbad30b-f590-4c0f-8f5d-941ff59a4043" name="In" connectedTo="79348c3e-87d7-4fea-9e89-d8f625889e8f"/>
        <port xsi:type="esdl:OutPort" id="88cbacd8-c06d-4cac-924b-5fdd5c1f8d38" name="Out" connectedTo="a4686b94-5cf3-4399-8298-27f204755db3 85d6aead-bf8b-4244-a9b8-a6844cca2aa7 8af984f4-1135-4e8d-945e-f769bec4ac44"/>
        <geometry xsi:type="esdl:Point" lat="54.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home7-1" length="1.0" assetType="lv_line_to_home" id="26b3ed9d-6cf9-4015-9b64-8298ab4d55bd">
        <port xsi:type="esdl:InPort" id="a4686b94-5cf3-4399-8298-27f204755db3" name="In" connectedTo="88cbacd8-c06d-4cac-924b-5fdd5c1f8d38"/>
        <port xsi:type="esdl:OutPort" id="3103f0c8-3d48-4622-9021-efa9c2d68a74" name="Out" connectedTo="f1632353-8515-4451-80b2-d0e8cf994ae6"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="54.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="54.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home7-2" length="1.0" assetType="lv_line_to_home" id="49db76c7-1f02-46d5-a373-f6e836c40ee7">
        <port xsi:type="esdl:InPort" id="85d6aead-bf8b-4244-a9b8-a6844cca2aa7" name="In" connectedTo="88cbacd8-c06d-4cac-924b-5fdd5c1f8d38"/>
        <port xsi:type="esdl:OutPort" id="3e87c92e-4838-4f92-b4d0-a75017ef01ad" name="Out" connectedTo="6f1d8d9c-cea7-4c5e-98dc-04abf4287b8b"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="54.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="54.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home7-1" id="5e34f777-9873-40a9-aeaf-596ed31e434f">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="e389c2db-6d75-4f8a-acf6-40e5137a2a37" connectedTo="b393c149-57ad-4be3-a1b3-f7b11cbc2f30" name="In"/>
          <port xsi:type="esdl:OutPort" id="aeceba11-82b2-475c-b8bd-1bb636256d0d" name="Out" connectedTo="9af5a48c-5701-4338-b334-288149d44e10"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="39f8cee3-0ce3-427c-bf30-23b1b41cee38" connectedTo="df69164b-04d7-4e0f-90f2-c9ad7722c0f2" name="In"/>
          <port xsi:type="esdl:OutPort" id="2a13fca4-d756-4d42-8a5d-5d12630dc630" name="Out" connectedTo="dc73cb3b-a4bd-464e-a09e-aa984c198057"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="3ccf2f31-4a84-4c95-96ea-043e55cfec6e" connectedTo="b694760f-7941-41ee-b285-b0d19643bc80" name="In"/>
          <port xsi:type="esdl:OutPort" id="7e1f3d05-0982-4897-ac49-dd459467c73d" name="Out" connectedTo="90b94b7d-1bbb-4f12-8d3e-e216dc30382e"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="b9865d31-ce7f-4f3e-b233-a2d2af03727f">
          <port xsi:type="esdl:InPort" id="9af5a48c-5701-4338-b334-288149d44e10" name="In_Ph1" connectedTo="aeceba11-82b2-475c-b8bd-1bb636256d0d"/>
          <port xsi:type="esdl:InPort" id="dc73cb3b-a4bd-464e-a09e-aa984c198057" name="In_Ph2" connectedTo="2a13fca4-d756-4d42-8a5d-5d12630dc630"/>
          <port xsi:type="esdl:InPort" id="90b94b7d-1bbb-4f12-8d3e-e216dc30382e" name="In_Ph3" connectedTo="7e1f3d05-0982-4897-ac49-dd459467c73d"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home7-1" id="1854c631-0073-44a0-a41b-9db8376bc115">
          <port xsi:type="esdl:InPort" id="6f1d8d9c-cea7-4c5e-98dc-04abf4287b8b" name="In" connectedTo="3e87c92e-4838-4f92-b4d0-a75017ef01ad"/>
          <port xsi:type="esdl:OutPort" id="b393c149-57ad-4be3-a1b3-f7b11cbc2f30" name="OutPh1" connectedTo="e389c2db-6d75-4f8a-acf6-40e5137a2a37"/>
          <port xsi:type="esdl:OutPort" id="df69164b-04d7-4e0f-90f2-c9ad7722c0f2" name="OutPh2" connectedTo="39f8cee3-0ce3-427c-bf30-23b1b41cee38"/>
          <port xsi:type="esdl:OutPort" id="b694760f-7941-41ee-b285-b0d19643bc80" name="OutPh3" connectedTo="3ccf2f31-4a84-4c95-96ea-043e55cfec6e"/>
          <geometry xsi:type="esdl:Point" lat="54.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home7-2" id="b8f0dfa1-5da8-4851-8413-3bf522cfde89">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="8d367d79-43d7-4bf7-bb60-72c744d32e75" connectedTo="6dda3ddd-969a-4e3a-b91f-c5fd60c6a75e" name="In"/>
          <port xsi:type="esdl:OutPort" id="3273f3b2-5efb-4ad1-be6d-8082a3eb47d7" name="Out" connectedTo="55b428ac-ebbd-462b-b5f2-fdb69a20d4c3"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="6af3935e-2d40-48c2-9ab7-00ef6a72bcb3" connectedTo="34f2d764-ad6a-4e1f-a5e0-c475081eff69" name="In"/>
          <port xsi:type="esdl:OutPort" id="076c55d2-46e1-4bed-a98a-ed1fedff1be7" name="Out" connectedTo="f62413d7-ce23-494a-a823-24db2fc030f6"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="9576a496-20ce-4954-b1df-fb571e4f3629" connectedTo="accf7d00-43a3-410d-a229-b6f9f638cfed" name="In"/>
          <port xsi:type="esdl:OutPort" id="119f2794-f3ed-449e-99f4-650024e4b8c8" name="Out" connectedTo="22a277f8-854a-41da-a3f7-8446ab479f95"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="27c12682-7f62-4fde-9c18-fdcfbb4b678e">
          <port xsi:type="esdl:InPort" id="55b428ac-ebbd-462b-b5f2-fdb69a20d4c3" name="In_Ph1" connectedTo="3273f3b2-5efb-4ad1-be6d-8082a3eb47d7"/>
          <port xsi:type="esdl:InPort" id="f62413d7-ce23-494a-a823-24db2fc030f6" name="In_Ph2" connectedTo="076c55d2-46e1-4bed-a98a-ed1fedff1be7"/>
          <port xsi:type="esdl:InPort" id="22a277f8-854a-41da-a3f7-8446ab479f95" name="In_Ph3" connectedTo="119f2794-f3ed-449e-99f4-650024e4b8c8"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home7-2" id="3a0a71d0-762b-4332-839a-92fd76d4a492">
          <port xsi:type="esdl:InPort" id="f1632353-8515-4451-80b2-d0e8cf994ae6" name="In" connectedTo="3103f0c8-3d48-4622-9021-efa9c2d68a74"/>
          <port xsi:type="esdl:OutPort" id="6dda3ddd-969a-4e3a-b91f-c5fd60c6a75e" name="OutPh1" connectedTo="8d367d79-43d7-4bf7-bb60-72c744d32e75"/>
          <port xsi:type="esdl:OutPort" id="34f2d764-ad6a-4e1f-a5e0-c475081eff69" name="OutPh2" connectedTo="6af3935e-2d40-48c2-9ab7-00ef6a72bcb3"/>
          <port xsi:type="esdl:OutPort" id="accf7d00-43a3-410d-a229-b6f9f638cfed" name="OutPh3" connectedTo="9576a496-20ce-4954-b1df-fb571e4f3629"/>
          <geometry xsi:type="esdl:Point" lat="54.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable8" length="6.0" assetType="lv_line" id="f1b465a0-7024-47c0-8ab0-b9110c7bc13d">
        <port xsi:type="esdl:InPort" id="8af984f4-1135-4e8d-945e-f769bec4ac44" name="In" connectedTo="88cbacd8-c06d-4cac-924b-5fdd5c1f8d38"/>
        <port xsi:type="esdl:OutPort" id="d9bab7a7-d930-479a-890a-3f687037d9f2" name="Out" connectedTo="af1edda7-79b1-4a67-9b8d-4752475dc536"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="54.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="60.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint8" id="056b01a4-5758-4204-a182-2b1bcb0d5304">
        <port xsi:type="esdl:InPort" id="af1edda7-79b1-4a67-9b8d-4752475dc536" name="In" connectedTo="d9bab7a7-d930-479a-890a-3f687037d9f2"/>
        <port xsi:type="esdl:OutPort" id="403e7713-d88b-4d20-a5e5-c2bc095aaf2e" name="Out" connectedTo="23e62c5f-a487-460d-a59c-a3699f2b704f 6fe76827-3e23-4cca-9d6a-01a5885e0e07 88ffda1f-804b-4b35-9176-4b0748cba435"/>
        <geometry xsi:type="esdl:Point" lat="60.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home8-1" length="1.0" assetType="lv_line_to_home" id="12a441f3-7dac-4725-8d14-a796d0205875">
        <port xsi:type="esdl:InPort" id="23e62c5f-a487-460d-a59c-a3699f2b704f" name="In" connectedTo="403e7713-d88b-4d20-a5e5-c2bc095aaf2e"/>
        <port xsi:type="esdl:OutPort" id="650ff6ce-d029-4687-9bcd-2377ee7d95f7" name="Out" connectedTo="147abdd5-d8e4-468e-8e48-a73a9a8f8604"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="60.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="60.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home8-2" length="1.0" assetType="lv_line_to_home" id="ec235f99-faaa-49a3-b8cd-bc6df6c82bbc">
        <port xsi:type="esdl:InPort" id="6fe76827-3e23-4cca-9d6a-01a5885e0e07" name="In" connectedTo="403e7713-d88b-4d20-a5e5-c2bc095aaf2e"/>
        <port xsi:type="esdl:OutPort" id="117880eb-0fe7-409f-bde3-7639dcf5a7a3" name="Out" connectedTo="d3fa7625-0065-498a-8a54-8f0192d71782"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="60.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="60.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home8-1" id="ade54e72-7561-4e81-b7dd-c11e6b00b9ca">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="bcef123f-fefa-4c35-a409-28d5042c87bd" connectedTo="77034e52-bfa0-4358-9e93-a501d85a0f64" name="In"/>
          <port xsi:type="esdl:OutPort" id="5707efbe-661e-4235-812e-e5d4b82dfc83" name="Out" connectedTo="10f7b636-8be8-4570-824e-1b9b4501c4f7"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="b1196894-dfd7-4228-9096-51c3d27dfe33" connectedTo="b70fa325-29df-4f49-a8c7-4070fcfd974d" name="In"/>
          <port xsi:type="esdl:OutPort" id="5fb79438-6db3-4651-b3a1-024199b75635" name="Out" connectedTo="ba2dd570-a54b-41f5-90a8-bc0cc53c918d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="914c21d5-95eb-47da-b199-74e3ba976b7f" connectedTo="837e7517-e753-4703-a5d9-f298cf833529" name="In"/>
          <port xsi:type="esdl:OutPort" id="59c32feb-24c5-49f3-bfbf-1d9cee181854" name="Out" connectedTo="e58cab87-b873-424c-ac23-9a5e6720da15"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="e34267a6-4f0c-4164-a7fa-5c40a92c1c57">
          <port xsi:type="esdl:InPort" id="10f7b636-8be8-4570-824e-1b9b4501c4f7" name="In_Ph1" connectedTo="5707efbe-661e-4235-812e-e5d4b82dfc83"/>
          <port xsi:type="esdl:InPort" id="ba2dd570-a54b-41f5-90a8-bc0cc53c918d" name="In_Ph2" connectedTo="5fb79438-6db3-4651-b3a1-024199b75635"/>
          <port xsi:type="esdl:InPort" id="e58cab87-b873-424c-ac23-9a5e6720da15" name="In_Ph3" connectedTo="59c32feb-24c5-49f3-bfbf-1d9cee181854"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home8-1" id="8338ac50-6049-495e-892b-a4dbeca67e19">
          <port xsi:type="esdl:InPort" id="d3fa7625-0065-498a-8a54-8f0192d71782" name="In" connectedTo="117880eb-0fe7-409f-bde3-7639dcf5a7a3"/>
          <port xsi:type="esdl:OutPort" id="77034e52-bfa0-4358-9e93-a501d85a0f64" name="OutPh1" connectedTo="bcef123f-fefa-4c35-a409-28d5042c87bd"/>
          <port xsi:type="esdl:OutPort" id="b70fa325-29df-4f49-a8c7-4070fcfd974d" name="OutPh2" connectedTo="b1196894-dfd7-4228-9096-51c3d27dfe33"/>
          <port xsi:type="esdl:OutPort" id="837e7517-e753-4703-a5d9-f298cf833529" name="OutPh3" connectedTo="914c21d5-95eb-47da-b199-74e3ba976b7f"/>
          <geometry xsi:type="esdl:Point" lat="60.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home8-2" id="427e0780-91ae-4f0b-ba10-9e75b3ae6375">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="34bf98dc-37ed-48b6-9b9e-da16cb8bd011" connectedTo="8cb07abc-8390-4755-99cf-620e831b7458" name="In"/>
          <port xsi:type="esdl:OutPort" id="a55b71b5-2b65-4097-9037-d996b39b4523" name="Out" connectedTo="34513090-d268-4e72-9de8-b22e5e6cfce2"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="a2c98714-155f-4aab-ac2f-35ad2a06dbd8" connectedTo="8a5676c8-e47e-44af-b7cd-38f59796e00d" name="In"/>
          <port xsi:type="esdl:OutPort" id="d51ec705-1e2e-4923-861f-72f8aef68683" name="Out" connectedTo="413eae1b-39a2-47f2-a2a8-2cb7460808dd"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="a708f1db-0d59-43cb-a345-7cce3060b979" connectedTo="7942d309-38f2-41cc-91a2-5df059186e67" name="In"/>
          <port xsi:type="esdl:OutPort" id="b84d4561-3fa0-48c1-8891-b4105d1bb357" name="Out" connectedTo="261cf9f9-093b-4615-be61-24d7179b4c96"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="0e61a2a2-029e-41e2-aad4-c282a50ed85a">
          <port xsi:type="esdl:InPort" id="34513090-d268-4e72-9de8-b22e5e6cfce2" name="In_Ph1" connectedTo="a55b71b5-2b65-4097-9037-d996b39b4523"/>
          <port xsi:type="esdl:InPort" id="413eae1b-39a2-47f2-a2a8-2cb7460808dd" name="In_Ph2" connectedTo="d51ec705-1e2e-4923-861f-72f8aef68683"/>
          <port xsi:type="esdl:InPort" id="261cf9f9-093b-4615-be61-24d7179b4c96" name="In_Ph3" connectedTo="b84d4561-3fa0-48c1-8891-b4105d1bb357"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home8-2" id="3e0d9a2a-8df2-4669-818a-95a8c938d508">
          <port xsi:type="esdl:InPort" id="147abdd5-d8e4-468e-8e48-a73a9a8f8604" name="In" connectedTo="650ff6ce-d029-4687-9bcd-2377ee7d95f7"/>
          <port xsi:type="esdl:OutPort" id="8cb07abc-8390-4755-99cf-620e831b7458" name="OutPh1" connectedTo="34bf98dc-37ed-48b6-9b9e-da16cb8bd011"/>
          <port xsi:type="esdl:OutPort" id="8a5676c8-e47e-44af-b7cd-38f59796e00d" name="OutPh2" connectedTo="a2c98714-155f-4aab-ac2f-35ad2a06dbd8"/>
          <port xsi:type="esdl:OutPort" id="7942d309-38f2-41cc-91a2-5df059186e67" name="OutPh3" connectedTo="a708f1db-0d59-43cb-a345-7cce3060b979"/>
          <geometry xsi:type="esdl:Point" lat="60.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable9" length="6.0" assetType="lv_line" id="8d7bd1a6-22ac-4919-be1e-ca8056f1847d">
        <port xsi:type="esdl:InPort" id="88ffda1f-804b-4b35-9176-4b0748cba435" name="In" connectedTo="403e7713-d88b-4d20-a5e5-c2bc095aaf2e"/>
        <port xsi:type="esdl:OutPort" id="83668ad7-ae07-4482-af1a-d80cda661f11" name="Out" connectedTo="34c78d27-5a9f-408b-9907-b0bac4e7d0b3"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="60.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="66.0" lon="12.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid1-joint9" id="b94eae04-7058-4a02-b9af-ebd3f995b128">
        <port xsi:type="esdl:InPort" id="34c78d27-5a9f-408b-9907-b0bac4e7d0b3" name="In" connectedTo="83668ad7-ae07-4482-af1a-d80cda661f11"/>
        <port xsi:type="esdl:OutPort" id="fae1297a-385c-4b25-af0c-fef053cf8984" name="Out" connectedTo="b0426220-127d-43b0-9639-664c646f8f7d a6724e10-6470-43e1-935c-eae9b597a091"/>
        <geometry xsi:type="esdl:Point" lat="66.0" lon="12.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home9-1" length="1.0" assetType="lv_line_to_home" id="7c727613-4a66-4e8d-b805-d2c814f376aa">
        <port xsi:type="esdl:InPort" id="b0426220-127d-43b0-9639-664c646f8f7d" name="In" connectedTo="fae1297a-385c-4b25-af0c-fef053cf8984"/>
        <port xsi:type="esdl:OutPort" id="45a88cda-24c5-448b-8bad-fcdc0afef662" name="Out" connectedTo="2ea36e03-e0a1-4cc2-a436-77beff13b1fa"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="66.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="66.0" lon="13.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid1-cable_to_home9-2" length="1.0" assetType="lv_line_to_home" id="8a11aef0-6f0f-4fa6-a93c-57b370dcc060">
        <port xsi:type="esdl:InPort" id="a6724e10-6470-43e1-935c-eae9b597a091" name="In" connectedTo="fae1297a-385c-4b25-af0c-fef053cf8984"/>
        <port xsi:type="esdl:OutPort" id="533415cd-a8fa-402d-8416-10b84bfc0cf3" name="Out" connectedTo="a9c209ca-3042-47ff-9bf4-efbd8327cb42"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="66.0" lon="12.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="66.0" lon="11.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home9-1" id="c514b5d2-a87f-42e6-809d-01eb0a5b1d84">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="f709e4fb-e71c-491f-a5b7-dda6e7bb7bdf" connectedTo="c2dafaac-5420-405d-a6df-21375858045c" name="In"/>
          <port xsi:type="esdl:OutPort" id="be236c54-3bc6-48f4-99c3-db827aa9de5b" name="Out" connectedTo="3c747ff0-acae-4219-80a3-79fc315de69a"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="1dd44f50-600c-4fdd-934e-aaf71d4497cd" connectedTo="d8fcf3ad-80c3-4bb2-b3c5-b293da7dea25" name="In"/>
          <port xsi:type="esdl:OutPort" id="eb43f49a-5e66-46e9-821f-dd54500d2276" name="Out" connectedTo="9904f504-d6f4-42b3-9155-8011a3cebce3"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="3fdc5167-ec37-4028-9026-e3e578a15abf" connectedTo="76c4ab03-867b-4015-a07d-80b4a9d53249" name="In"/>
          <port xsi:type="esdl:OutPort" id="c5a56323-18ab-40de-b4a9-e9130fe8a567" name="Out" connectedTo="a6daaf50-381f-4f8b-b052-268b2ef0038e"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="3235ac25-2472-48e7-a8ca-7b606877a844">
          <port xsi:type="esdl:InPort" id="3c747ff0-acae-4219-80a3-79fc315de69a" name="In_Ph1" connectedTo="be236c54-3bc6-48f4-99c3-db827aa9de5b"/>
          <port xsi:type="esdl:InPort" id="9904f504-d6f4-42b3-9155-8011a3cebce3" name="In_Ph2" connectedTo="eb43f49a-5e66-46e9-821f-dd54500d2276"/>
          <port xsi:type="esdl:InPort" id="a6daaf50-381f-4f8b-b052-268b2ef0038e" name="In_Ph3" connectedTo="c5a56323-18ab-40de-b4a9-e9130fe8a567"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home9-1" id="6047ed39-34f9-4827-9cdd-8f7c9910867b">
          <port xsi:type="esdl:InPort" id="a9c209ca-3042-47ff-9bf4-efbd8327cb42" name="In" connectedTo="533415cd-a8fa-402d-8416-10b84bfc0cf3"/>
          <port xsi:type="esdl:OutPort" id="c2dafaac-5420-405d-a6df-21375858045c" name="OutPh1" connectedTo="f709e4fb-e71c-491f-a5b7-dda6e7bb7bdf"/>
          <port xsi:type="esdl:OutPort" id="d8fcf3ad-80c3-4bb2-b3c5-b293da7dea25" name="OutPh2" connectedTo="1dd44f50-600c-4fdd-934e-aaf71d4497cd"/>
          <port xsi:type="esdl:OutPort" id="76c4ab03-867b-4015-a07d-80b4a9d53249" name="OutPh3" connectedTo="3fdc5167-ec37-4028-9026-e3e578a15abf"/>
          <geometry xsi:type="esdl:Point" lat="66.0" lon="13.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-1-home9-2" id="27d2bee8-e268-423a-8941-d68783313df3">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="a5813512-7955-4032-8dec-0d5c147d62f9" connectedTo="f4d1361b-25a7-43bf-bb44-44ccb3e7749a" name="In"/>
          <port xsi:type="esdl:OutPort" id="087423e3-058d-43b4-a255-9332390be4c8" name="Out" connectedTo="e512fee2-fc5f-47be-915b-b5b39f3518f5"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="bc867863-d6c5-47cb-afec-7ad74c584aef" connectedTo="12ef6132-8eaf-4072-8159-a64ad9cc42fa" name="In"/>
          <port xsi:type="esdl:OutPort" id="0faca5a5-107f-4247-b459-292358d017f5" name="Out" connectedTo="6a497e06-096a-428a-ab5a-d9fa06e7f0ce"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="f7babe88-6c68-4ee9-9643-ec7819a52386" connectedTo="22a14dad-dd44-4437-a7c5-fe6c7cecac64" name="In"/>
          <port xsi:type="esdl:OutPort" id="7351c69c-cda9-485b-a9a2-9584399628c3" name="Out" connectedTo="ee4df1eb-07e0-4fe1-a6b5-edd7e6cc185d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="f779553c-3793-40f4-a203-c4ae424eecbc">
          <port xsi:type="esdl:InPort" id="e512fee2-fc5f-47be-915b-b5b39f3518f5" name="In_Ph1" connectedTo="087423e3-058d-43b4-a255-9332390be4c8"/>
          <port xsi:type="esdl:InPort" id="6a497e06-096a-428a-ab5a-d9fa06e7f0ce" name="In_Ph2" connectedTo="0faca5a5-107f-4247-b459-292358d017f5"/>
          <port xsi:type="esdl:InPort" id="ee4df1eb-07e0-4fe1-a6b5-edd7e6cc185d" name="In_Ph3" connectedTo="7351c69c-cda9-485b-a9a2-9584399628c3"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-1-home9-2" id="0acaa05f-5b73-4dc2-a262-9309fcfa7b0d">
          <port xsi:type="esdl:InPort" id="2ea36e03-e0a1-4cc2-a436-77beff13b1fa" name="In" connectedTo="45a88cda-24c5-448b-8bad-fcdc0afef662"/>
          <port xsi:type="esdl:OutPort" id="f4d1361b-25a7-43bf-bb44-44ccb3e7749a" name="OutPh1" connectedTo="a5813512-7955-4032-8dec-0d5c147d62f9"/>
          <port xsi:type="esdl:OutPort" id="12ef6132-8eaf-4072-8159-a64ad9cc42fa" name="OutPh2" connectedTo="bc867863-d6c5-47cb-afec-7ad74c584aef"/>
          <port xsi:type="esdl:OutPort" id="22a14dad-dd44-4437-a7c5-fe6c7cecac64" name="OutPh3" connectedTo="f7babe88-6c68-4ee9-9643-ec7819a52386"/>
          <geometry xsi:type="esdl:Point" lat="66.0" lon="11.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable0" length="6.0" assetType="lv_line" id="b6ea4508-9662-43f8-aa98-3cd19dd1a6e0">
        <port xsi:type="esdl:InPort" id="30255e01-8679-4aea-a267-b1704a747862" name="In" connectedTo="970831cf-9cff-4f9d-95f2-a2709cedf914"/>
        <port xsi:type="esdl:OutPort" id="75bfd40c-6eea-42fe-95ef-3107ab8736f0" name="Out" connectedTo="bd16d13a-ae62-4099-95f2-fb7eaa0c4f86"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="12.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid2-joint0" id="c90b54aa-7228-4c8a-a3ce-3bf13de24db1">
        <port xsi:type="esdl:InPort" id="bd16d13a-ae62-4099-95f2-fb7eaa0c4f86" name="In" connectedTo="75bfd40c-6eea-42fe-95ef-3107ab8736f0"/>
        <port xsi:type="esdl:OutPort" id="c63073e5-874b-428c-a8c7-8d31029052ac" name="Out" connectedTo="51b88d98-9836-4520-8e57-9bfd176d38aa 0c9929b0-d41f-4648-81de-3955ea65850a f7d4e1c4-706f-43f1-82f6-df0f0df6533e"/>
        <geometry xsi:type="esdl:Point" lat="18.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home0-1" length="1.0" assetType="lv_line_to_home" id="4a0a4a03-aeb4-4b83-b608-79e6e8de88f4">
        <port xsi:type="esdl:InPort" id="51b88d98-9836-4520-8e57-9bfd176d38aa" name="In" connectedTo="c63073e5-874b-428c-a8c7-8d31029052ac"/>
        <port xsi:type="esdl:OutPort" id="56215709-8a5a-41f8-b4ca-285b3507b6de" name="Out" connectedTo="ab69f711-3c92-45cd-8d4f-66b634205afd"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="25.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home0-2" length="1.0" assetType="lv_line_to_home" id="1de96492-e197-42d9-8730-5224fa9d550e">
        <port xsi:type="esdl:InPort" id="0c9929b0-d41f-4648-81de-3955ea65850a" name="In" connectedTo="c63073e5-874b-428c-a8c7-8d31029052ac"/>
        <port xsi:type="esdl:OutPort" id="6fff63b2-4f95-4cb0-82da-a27176a4551b" name="Out" connectedTo="489a0c23-6d6a-44c3-99b9-ce65925cbfb3"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="23.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home0-1" id="999113f4-cb88-47cd-a275-e11354b12a1c">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="db5b3fa0-3ee5-48ef-858f-af22afa404e2" connectedTo="a5fbe1d7-67c0-4c9e-9f71-f68618b94b94" name="In"/>
          <port xsi:type="esdl:OutPort" id="750042f4-4180-4b33-bb37-ddb694c68143" name="Out" connectedTo="7acffb57-f2b7-4e7d-bffb-85f5a14dc898"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="13a36c96-76d0-4c77-a990-7e19ce6832cb" connectedTo="cbe9bb2a-aba6-47c4-b38f-c9a0960d2bb3" name="In"/>
          <port xsi:type="esdl:OutPort" id="5b1c580a-be67-4143-a491-2c18dcaeca12" name="Out" connectedTo="7a7a06ea-26fd-4875-a7ba-37c1468364d1"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="2353d48c-f38e-4089-98e3-fc37ba0837d6" connectedTo="008aae6e-07c0-44b5-a09e-013419eb9baa" name="In"/>
          <port xsi:type="esdl:OutPort" id="a8e98721-0897-4f7e-9179-e4c9aa6f1ee1" name="Out" connectedTo="aaaa0566-6228-436d-a8e4-6de8220113d8"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="35ff9b38-fe23-47fd-b05e-b051601c466d">
          <port xsi:type="esdl:InPort" id="7acffb57-f2b7-4e7d-bffb-85f5a14dc898" name="In_Ph1" connectedTo="750042f4-4180-4b33-bb37-ddb694c68143"/>
          <port xsi:type="esdl:InPort" id="7a7a06ea-26fd-4875-a7ba-37c1468364d1" name="In_Ph2" connectedTo="5b1c580a-be67-4143-a491-2c18dcaeca12"/>
          <port xsi:type="esdl:InPort" id="aaaa0566-6228-436d-a8e4-6de8220113d8" name="In_Ph3" connectedTo="a8e98721-0897-4f7e-9179-e4c9aa6f1ee1"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home0-1" id="53c25d66-2611-4c35-9d9d-e4b5378961c4">
          <port xsi:type="esdl:InPort" id="489a0c23-6d6a-44c3-99b9-ce65925cbfb3" name="In" connectedTo="6fff63b2-4f95-4cb0-82da-a27176a4551b"/>
          <port xsi:type="esdl:OutPort" id="a5fbe1d7-67c0-4c9e-9f71-f68618b94b94" name="OutPh1" connectedTo="db5b3fa0-3ee5-48ef-858f-af22afa404e2"/>
          <port xsi:type="esdl:OutPort" id="cbe9bb2a-aba6-47c4-b38f-c9a0960d2bb3" name="OutPh2" connectedTo="13a36c96-76d0-4c77-a990-7e19ce6832cb"/>
          <port xsi:type="esdl:OutPort" id="008aae6e-07c0-44b5-a09e-013419eb9baa" name="OutPh3" connectedTo="2353d48c-f38e-4089-98e3-fc37ba0837d6"/>
          <geometry xsi:type="esdl:Point" lat="18.0" lon="25.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home0-2" id="178844a7-5838-4a29-8e87-b9a0520c0097">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="9da9fd77-6055-41a7-a5fc-f0a4835c8174" connectedTo="c3162e7c-dd2d-4615-bf79-2827d2186727" name="In"/>
          <port xsi:type="esdl:OutPort" id="e64fecfd-4a6d-497d-a288-7f5e7b7464fe" name="Out" connectedTo="da682e2e-51d4-4fc9-ab97-0811d6e9d1c0"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="b63f83ac-29aa-4997-85bd-b36467a218e8" connectedTo="5802c86f-5926-4d6a-a490-840843787fd8" name="In"/>
          <port xsi:type="esdl:OutPort" id="212f6c35-83e4-4142-8cc5-e3005dd2d0db" name="Out" connectedTo="a0630230-0360-4c9f-8239-3a09365d5178"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="b9dee21f-02a4-48c7-84f6-5f6560f4a8f6" connectedTo="036b4846-79bd-4417-b7b3-63269439d8cd" name="In"/>
          <port xsi:type="esdl:OutPort" id="20bda6e8-e01e-42b8-b94d-d12e5a0e4702" name="Out" connectedTo="68918f33-f67d-4779-b52a-10dafc6b0a64"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="312d2d48-ac6f-41be-b395-d73edc695e01">
          <port xsi:type="esdl:InPort" id="da682e2e-51d4-4fc9-ab97-0811d6e9d1c0" name="In_Ph1" connectedTo="e64fecfd-4a6d-497d-a288-7f5e7b7464fe"/>
          <port xsi:type="esdl:InPort" id="a0630230-0360-4c9f-8239-3a09365d5178" name="In_Ph2" connectedTo="212f6c35-83e4-4142-8cc5-e3005dd2d0db"/>
          <port xsi:type="esdl:InPort" id="68918f33-f67d-4779-b52a-10dafc6b0a64" name="In_Ph3" connectedTo="20bda6e8-e01e-42b8-b94d-d12e5a0e4702"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home0-2" id="bea09aea-4c27-4aed-985e-533f1ca4efde">
          <port xsi:type="esdl:InPort" id="ab69f711-3c92-45cd-8d4f-66b634205afd" name="In" connectedTo="56215709-8a5a-41f8-b4ca-285b3507b6de"/>
          <port xsi:type="esdl:OutPort" id="c3162e7c-dd2d-4615-bf79-2827d2186727" name="OutPh1" connectedTo="9da9fd77-6055-41a7-a5fc-f0a4835c8174"/>
          <port xsi:type="esdl:OutPort" id="5802c86f-5926-4d6a-a490-840843787fd8" name="OutPh2" connectedTo="b63f83ac-29aa-4997-85bd-b36467a218e8"/>
          <port xsi:type="esdl:OutPort" id="036b4846-79bd-4417-b7b3-63269439d8cd" name="OutPh3" connectedTo="b9dee21f-02a4-48c7-84f6-5f6560f4a8f6"/>
          <geometry xsi:type="esdl:Point" lat="18.0" lon="23.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable1" length="6.0" assetType="lv_line" id="c8cb2e00-6176-49da-96b0-0d312a62dc58">
        <port xsi:type="esdl:InPort" id="f7d4e1c4-706f-43f1-82f6-df0f0df6533e" name="In" connectedTo="c63073e5-874b-428c-a8c7-8d31029052ac"/>
        <port xsi:type="esdl:OutPort" id="1062f2c9-f1e5-499a-afbb-b3a54105573f" name="Out" connectedTo="aa36ea5a-0b62-47c1-abd6-f22841355bd7"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid2-joint1" id="3e4cfdad-4539-4a7d-b9c5-4c11a6eea396">
        <port xsi:type="esdl:InPort" id="aa36ea5a-0b62-47c1-abd6-f22841355bd7" name="In" connectedTo="1062f2c9-f1e5-499a-afbb-b3a54105573f"/>
        <port xsi:type="esdl:OutPort" id="618f58a2-090d-4648-bf5c-1bad49cfbfb5" name="Out" connectedTo="0f122d8f-f518-4b11-a894-1fceb0a04fb5 aec83b2b-41d2-460b-a34e-ca4475d8151e e337da4a-0504-43d2-b66a-7d954b9504ab"/>
        <geometry xsi:type="esdl:Point" lat="24.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home1-1" length="1.0" assetType="lv_line_to_home" id="2769fc23-4edd-4f00-ae9b-9f296149cab6">
        <port xsi:type="esdl:InPort" id="0f122d8f-f518-4b11-a894-1fceb0a04fb5" name="In" connectedTo="618f58a2-090d-4648-bf5c-1bad49cfbfb5"/>
        <port xsi:type="esdl:OutPort" id="d2017d42-2864-4961-bbd2-4a842235bcc0" name="Out" connectedTo="eedff085-5b17-41f1-b2b0-d725c16a4187"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="25.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home1-2" length="1.0" assetType="lv_line_to_home" id="19ab1fc8-47a7-4a6d-9a7e-313c7f0ba2ba">
        <port xsi:type="esdl:InPort" id="aec83b2b-41d2-460b-a34e-ca4475d8151e" name="In" connectedTo="618f58a2-090d-4648-bf5c-1bad49cfbfb5"/>
        <port xsi:type="esdl:OutPort" id="949d5ed2-0dfe-474e-b2ad-d4513fb90645" name="Out" connectedTo="4aa5b473-9f4e-4cc9-8f26-8cab3a5fa3d1"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="23.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home1-1" id="9f986c8c-5846-4694-be79-12da87e2b615">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="74824498-c191-46d4-9c89-8c999de18f79" connectedTo="59fcd6ef-f5a4-4f7e-9b33-dc80a0a38a17" name="In"/>
          <port xsi:type="esdl:OutPort" id="272e038e-1ed0-48e3-b916-afec068e1fb5" name="Out" connectedTo="29c11cbb-99a6-49b5-b514-4f7e9997681f"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="398cb887-7f44-4bc6-a84d-6abed05bf95b" connectedTo="0499f867-3ed6-468a-815b-f6f5c4c9af7a" name="In"/>
          <port xsi:type="esdl:OutPort" id="c31e388e-93c2-4cc8-a107-55decc468e81" name="Out" connectedTo="7f9fc361-a9e8-4a67-93eb-61ba2b537c2c"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="7ea97670-d997-4741-97f5-fc7adcd3b9d2" connectedTo="980dd719-f1f5-41c4-a600-1b202ecddcf8" name="In"/>
          <port xsi:type="esdl:OutPort" id="e16123a0-ce5a-4d79-9c24-e7e5f5c5499e" name="Out" connectedTo="e4ba1e83-470d-4fa9-92f0-af3e75c9f725"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="0a2ad2ce-6997-4834-a0eb-93ed92141f7a">
          <port xsi:type="esdl:InPort" id="29c11cbb-99a6-49b5-b514-4f7e9997681f" name="In_Ph1" connectedTo="272e038e-1ed0-48e3-b916-afec068e1fb5"/>
          <port xsi:type="esdl:InPort" id="7f9fc361-a9e8-4a67-93eb-61ba2b537c2c" name="In_Ph2" connectedTo="c31e388e-93c2-4cc8-a107-55decc468e81"/>
          <port xsi:type="esdl:InPort" id="e4ba1e83-470d-4fa9-92f0-af3e75c9f725" name="In_Ph3" connectedTo="e16123a0-ce5a-4d79-9c24-e7e5f5c5499e"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home1-1" id="048e5a4e-14c6-4f6c-944e-8fbc8996ce6a">
          <port xsi:type="esdl:InPort" id="4aa5b473-9f4e-4cc9-8f26-8cab3a5fa3d1" name="In" connectedTo="949d5ed2-0dfe-474e-b2ad-d4513fb90645"/>
          <port xsi:type="esdl:OutPort" id="59fcd6ef-f5a4-4f7e-9b33-dc80a0a38a17" name="OutPh1" connectedTo="74824498-c191-46d4-9c89-8c999de18f79"/>
          <port xsi:type="esdl:OutPort" id="0499f867-3ed6-468a-815b-f6f5c4c9af7a" name="OutPh2" connectedTo="398cb887-7f44-4bc6-a84d-6abed05bf95b"/>
          <port xsi:type="esdl:OutPort" id="980dd719-f1f5-41c4-a600-1b202ecddcf8" name="OutPh3" connectedTo="7ea97670-d997-4741-97f5-fc7adcd3b9d2"/>
          <geometry xsi:type="esdl:Point" lat="24.0" lon="25.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home1-2" id="44408fdd-f45b-4aa2-8396-a2bbb1dfa372">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="4fda7d1f-1387-43f8-8273-77cbf26adb29" connectedTo="980a4537-ec8a-4b9d-a3af-96143c906d9e" name="In"/>
          <port xsi:type="esdl:OutPort" id="7cddb30a-7c1d-4bcf-912a-4948db29eb8f" name="Out" connectedTo="f6bea1ec-c91c-4932-a62c-a3508eed2815"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="9b42e072-dafe-494e-847c-5adcd075ce4a" connectedTo="2ac5cf26-4768-4e24-8dee-f7ccb14bad5e" name="In"/>
          <port xsi:type="esdl:OutPort" id="9cdfadd1-238a-40af-9943-c466c82b2a33" name="Out" connectedTo="8e65d9b6-26f5-40aa-acfe-f4b343ca037d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="a32216dd-d1e8-46be-bcd7-9752f95396b5" connectedTo="acc89cd7-c676-42b6-b718-9b7e4102be6f" name="In"/>
          <port xsi:type="esdl:OutPort" id="4595814e-cc8a-4b13-8ef4-90f26d113b6a" name="Out" connectedTo="5529cc35-9b4a-4d10-b342-b49155d85726"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="0613ca09-7bd7-4413-ba8c-6972d4e4cd73">
          <port xsi:type="esdl:InPort" id="f6bea1ec-c91c-4932-a62c-a3508eed2815" name="In_Ph1" connectedTo="7cddb30a-7c1d-4bcf-912a-4948db29eb8f"/>
          <port xsi:type="esdl:InPort" id="8e65d9b6-26f5-40aa-acfe-f4b343ca037d" name="In_Ph2" connectedTo="9cdfadd1-238a-40af-9943-c466c82b2a33"/>
          <port xsi:type="esdl:InPort" id="5529cc35-9b4a-4d10-b342-b49155d85726" name="In_Ph3" connectedTo="4595814e-cc8a-4b13-8ef4-90f26d113b6a"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home1-2" id="b50a8654-0dde-463f-a62a-8861962ea0fe">
          <port xsi:type="esdl:InPort" id="eedff085-5b17-41f1-b2b0-d725c16a4187" name="In" connectedTo="d2017d42-2864-4961-bbd2-4a842235bcc0"/>
          <port xsi:type="esdl:OutPort" id="980a4537-ec8a-4b9d-a3af-96143c906d9e" name="OutPh1" connectedTo="4fda7d1f-1387-43f8-8273-77cbf26adb29"/>
          <port xsi:type="esdl:OutPort" id="2ac5cf26-4768-4e24-8dee-f7ccb14bad5e" name="OutPh2" connectedTo="9b42e072-dafe-494e-847c-5adcd075ce4a"/>
          <port xsi:type="esdl:OutPort" id="acc89cd7-c676-42b6-b718-9b7e4102be6f" name="OutPh3" connectedTo="a32216dd-d1e8-46be-bcd7-9752f95396b5"/>
          <geometry xsi:type="esdl:Point" lat="24.0" lon="23.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable2" length="6.0" assetType="lv_line" id="05e4b973-ceac-4416-a615-1d07c2bc9f84">
        <port xsi:type="esdl:InPort" id="e337da4a-0504-43d2-b66a-7d954b9504ab" name="In" connectedTo="618f58a2-090d-4648-bf5c-1bad49cfbfb5"/>
        <port xsi:type="esdl:OutPort" id="1cf95218-c2a6-4744-8ba7-b8285448285d" name="Out" connectedTo="a75d0ea0-8d6c-4732-804e-d990b48fd620"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid2-joint2" id="f82446b7-8991-4dd6-97dc-de91bbcd8ccd">
        <port xsi:type="esdl:InPort" id="a75d0ea0-8d6c-4732-804e-d990b48fd620" name="In" connectedTo="1cf95218-c2a6-4744-8ba7-b8285448285d"/>
        <port xsi:type="esdl:OutPort" id="8770594d-d708-44cd-ae2d-c6ba0d05be36" name="Out" connectedTo="1bf483ae-c5e9-4165-b5f4-466e785bcbdd 2f76cad6-2ef5-4d7c-aef8-251eef43d88a 2187855f-a967-4381-85be-9cf5c34b2a9b"/>
        <geometry xsi:type="esdl:Point" lat="30.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home2-1" length="1.0" assetType="lv_line_to_home" id="38187513-8529-4465-bfdf-750adcaa8ca0">
        <port xsi:type="esdl:InPort" id="1bf483ae-c5e9-4165-b5f4-466e785bcbdd" name="In" connectedTo="8770594d-d708-44cd-ae2d-c6ba0d05be36"/>
        <port xsi:type="esdl:OutPort" id="ac4ee39d-d2ec-4e28-a7dc-69639cd369ee" name="Out" connectedTo="3e4ade1a-1f02-42c5-b3cd-94ebaef30a93"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="25.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home2-2" length="1.0" assetType="lv_line_to_home" id="5f84c029-49aa-4ccf-ab3d-c21c4ace8b3e">
        <port xsi:type="esdl:InPort" id="2f76cad6-2ef5-4d7c-aef8-251eef43d88a" name="In" connectedTo="8770594d-d708-44cd-ae2d-c6ba0d05be36"/>
        <port xsi:type="esdl:OutPort" id="762ca028-5765-4a3f-b789-2df2e21f37f0" name="Out" connectedTo="cab1aa02-e5bc-48d6-8873-31a5692da084"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="23.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home2-1" id="c283338b-4c9d-43ab-961e-46a58d85d10c">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="53278752-f967-408b-8529-3b13915952af" connectedTo="f63add16-5a53-4a38-b7f5-4cae8778dc93" name="In"/>
          <port xsi:type="esdl:OutPort" id="f2a542fb-f698-48c6-821a-7f98c0cdc0f3" name="Out" connectedTo="b5ee08f6-d766-4a0b-8047-73f7cdee591a"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="2c4e6ca7-2efc-4b2e-bf6f-19510a4f5fdc" connectedTo="4278fdf2-5f5d-49ee-98a3-5f9b58d79e90" name="In"/>
          <port xsi:type="esdl:OutPort" id="40ce55de-0dab-4b9c-a2db-4df020d59aed" name="Out" connectedTo="1f56c0bb-8ca8-429b-9075-0eabd881c5ed"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="0deb1587-608f-4e40-842d-c942cd45e808" connectedTo="11cc22f3-01d5-45bc-8362-7f19e712889a" name="In"/>
          <port xsi:type="esdl:OutPort" id="dc8fa806-9c7a-46c9-bfdd-205d84f40100" name="Out" connectedTo="70da0238-3b08-418e-8f65-282c83c8f582"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="d3372996-6251-4d16-b82e-2b62a5387875">
          <port xsi:type="esdl:InPort" id="b5ee08f6-d766-4a0b-8047-73f7cdee591a" name="In_Ph1" connectedTo="f2a542fb-f698-48c6-821a-7f98c0cdc0f3"/>
          <port xsi:type="esdl:InPort" id="1f56c0bb-8ca8-429b-9075-0eabd881c5ed" name="In_Ph2" connectedTo="40ce55de-0dab-4b9c-a2db-4df020d59aed"/>
          <port xsi:type="esdl:InPort" id="70da0238-3b08-418e-8f65-282c83c8f582" name="In_Ph3" connectedTo="dc8fa806-9c7a-46c9-bfdd-205d84f40100"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home2-1" id="9dd2ac81-a8d2-499e-9973-8746f6218b6f">
          <port xsi:type="esdl:InPort" id="cab1aa02-e5bc-48d6-8873-31a5692da084" name="In" connectedTo="762ca028-5765-4a3f-b789-2df2e21f37f0"/>
          <port xsi:type="esdl:OutPort" id="f63add16-5a53-4a38-b7f5-4cae8778dc93" name="OutPh1" connectedTo="53278752-f967-408b-8529-3b13915952af"/>
          <port xsi:type="esdl:OutPort" id="4278fdf2-5f5d-49ee-98a3-5f9b58d79e90" name="OutPh2" connectedTo="2c4e6ca7-2efc-4b2e-bf6f-19510a4f5fdc"/>
          <port xsi:type="esdl:OutPort" id="11cc22f3-01d5-45bc-8362-7f19e712889a" name="OutPh3" connectedTo="0deb1587-608f-4e40-842d-c942cd45e808"/>
          <geometry xsi:type="esdl:Point" lat="30.0" lon="25.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home2-2" id="90e8153c-81b9-43d7-8ad1-6910d178d5ec">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="3a2be56d-f6e7-45af-b00f-97932692ec34" connectedTo="ad36ee83-7901-4654-809a-489417d9b7b6" name="In"/>
          <port xsi:type="esdl:OutPort" id="9d90d2e7-8b14-4ba3-8e8d-ebced4ff1b88" name="Out" connectedTo="a476ece5-313f-4f2f-9506-241473fc69b6"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="00c59c2b-9380-4050-9676-65d681e0c031" connectedTo="0216f9ca-6f07-4813-839b-467434727fce" name="In"/>
          <port xsi:type="esdl:OutPort" id="617e05e7-b230-4bcf-8471-396522469a8f" name="Out" connectedTo="29cb60dc-6af8-4d1e-979f-a40d4c3632b5"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="3e73408c-afb7-47d1-9430-feed9a1fab38" connectedTo="c1525441-7d7c-46ee-a073-6161ed043846" name="In"/>
          <port xsi:type="esdl:OutPort" id="14c86217-c6de-48bb-b97e-a165a79b8a5e" name="Out" connectedTo="dbbf112b-56e7-43e9-b209-4c765a0f97cf"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="355c0158-cb78-42d7-bd6f-530de415aceb">
          <port xsi:type="esdl:InPort" id="a476ece5-313f-4f2f-9506-241473fc69b6" name="In_Ph1" connectedTo="9d90d2e7-8b14-4ba3-8e8d-ebced4ff1b88"/>
          <port xsi:type="esdl:InPort" id="29cb60dc-6af8-4d1e-979f-a40d4c3632b5" name="In_Ph2" connectedTo="617e05e7-b230-4bcf-8471-396522469a8f"/>
          <port xsi:type="esdl:InPort" id="dbbf112b-56e7-43e9-b209-4c765a0f97cf" name="In_Ph3" connectedTo="14c86217-c6de-48bb-b97e-a165a79b8a5e"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home2-2" id="8e922e85-8634-461a-aad3-d0f40890f6bc">
          <port xsi:type="esdl:InPort" id="3e4ade1a-1f02-42c5-b3cd-94ebaef30a93" name="In" connectedTo="ac4ee39d-d2ec-4e28-a7dc-69639cd369ee"/>
          <port xsi:type="esdl:OutPort" id="ad36ee83-7901-4654-809a-489417d9b7b6" name="OutPh1" connectedTo="3a2be56d-f6e7-45af-b00f-97932692ec34"/>
          <port xsi:type="esdl:OutPort" id="0216f9ca-6f07-4813-839b-467434727fce" name="OutPh2" connectedTo="00c59c2b-9380-4050-9676-65d681e0c031"/>
          <port xsi:type="esdl:OutPort" id="c1525441-7d7c-46ee-a073-6161ed043846" name="OutPh3" connectedTo="3e73408c-afb7-47d1-9430-feed9a1fab38"/>
          <geometry xsi:type="esdl:Point" lat="30.0" lon="23.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable3" length="6.0" assetType="lv_line" id="e7dfa25b-eba3-40e6-b496-638c53bf5bb9">
        <port xsi:type="esdl:InPort" id="2187855f-a967-4381-85be-9cf5c34b2a9b" name="In" connectedTo="8770594d-d708-44cd-ae2d-c6ba0d05be36"/>
        <port xsi:type="esdl:OutPort" id="9e387746-33f8-4a00-9938-805dcf28f978" name="Out" connectedTo="4cc34c0b-eeee-402f-a474-fff5d9fcf882"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid2-joint3" id="36397d17-7e95-47d1-b055-421a35b9a5fa">
        <port xsi:type="esdl:InPort" id="4cc34c0b-eeee-402f-a474-fff5d9fcf882" name="In" connectedTo="9e387746-33f8-4a00-9938-805dcf28f978"/>
        <port xsi:type="esdl:OutPort" id="a89ac4a9-8afa-476c-a21d-4a112aedee26" name="Out" connectedTo="ee8bfdb6-5134-4b37-8607-1bdb274cc76e 1c95c276-634d-4f2c-995f-0c54c82ce1ef ee41a942-b8b6-4d54-84b7-5dc8aa26fb2a"/>
        <geometry xsi:type="esdl:Point" lat="36.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home3-1" length="1.0" assetType="lv_line_to_home" id="afd30ea2-1d09-42b7-884d-a1891298138a">
        <port xsi:type="esdl:InPort" id="ee8bfdb6-5134-4b37-8607-1bdb274cc76e" name="In" connectedTo="a89ac4a9-8afa-476c-a21d-4a112aedee26"/>
        <port xsi:type="esdl:OutPort" id="3d09925a-6ac9-4148-84f2-5859f9abddd5" name="Out" connectedTo="27973cea-eb61-4761-945a-ad7b5d4c490f"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="25.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home3-2" length="1.0" assetType="lv_line_to_home" id="7cdee41b-7175-4eb4-adf8-f68ed0908273">
        <port xsi:type="esdl:InPort" id="1c95c276-634d-4f2c-995f-0c54c82ce1ef" name="In" connectedTo="a89ac4a9-8afa-476c-a21d-4a112aedee26"/>
        <port xsi:type="esdl:OutPort" id="f4d806f9-2495-40c8-82fa-fb40c70400a4" name="Out" connectedTo="17eac104-a8e2-445e-af74-427a3a4daff9"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="23.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home3-1" id="4d5d7e3e-3950-414e-9acc-505b470f8eea">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="28ce41a1-4628-436e-84d0-21425aa6921a" connectedTo="3078534a-39e9-4f0a-aca4-052dfede5d8b" name="In"/>
          <port xsi:type="esdl:OutPort" id="39970c84-a388-4c8b-a186-aeaa687653c9" name="Out" connectedTo="c98b13f8-0a72-491a-8ba7-c640e0731373"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="63db14d3-195d-4cbb-b7c4-98c72c014463" connectedTo="a2c29b6e-9e7d-444a-8891-84dccbc6d426" name="In"/>
          <port xsi:type="esdl:OutPort" id="df01a8af-32b4-404e-800f-21cf5c28c82f" name="Out" connectedTo="f39e701f-3303-4354-a9ae-b5aea9705703"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="b519fd60-4add-411b-b4b5-f13ce3325897" connectedTo="82d8216a-17d1-4c15-b48e-11c0737b3e08" name="In"/>
          <port xsi:type="esdl:OutPort" id="81c7d304-6c6a-4252-bf7f-896c50f4bd28" name="Out" connectedTo="23b7ef19-cb39-45a4-a657-567b6d4060d6"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="72302552-cd04-4b20-93ad-6b0b268e3e4f">
          <port xsi:type="esdl:InPort" id="c98b13f8-0a72-491a-8ba7-c640e0731373" name="In_Ph1" connectedTo="39970c84-a388-4c8b-a186-aeaa687653c9"/>
          <port xsi:type="esdl:InPort" id="f39e701f-3303-4354-a9ae-b5aea9705703" name="In_Ph2" connectedTo="df01a8af-32b4-404e-800f-21cf5c28c82f"/>
          <port xsi:type="esdl:InPort" id="23b7ef19-cb39-45a4-a657-567b6d4060d6" name="In_Ph3" connectedTo="81c7d304-6c6a-4252-bf7f-896c50f4bd28"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home3-1" id="90ab6aaf-655d-4268-8b02-e55d96b40ddb">
          <port xsi:type="esdl:InPort" id="17eac104-a8e2-445e-af74-427a3a4daff9" name="In" connectedTo="f4d806f9-2495-40c8-82fa-fb40c70400a4"/>
          <port xsi:type="esdl:OutPort" id="3078534a-39e9-4f0a-aca4-052dfede5d8b" name="OutPh1" connectedTo="28ce41a1-4628-436e-84d0-21425aa6921a"/>
          <port xsi:type="esdl:OutPort" id="a2c29b6e-9e7d-444a-8891-84dccbc6d426" name="OutPh2" connectedTo="63db14d3-195d-4cbb-b7c4-98c72c014463"/>
          <port xsi:type="esdl:OutPort" id="82d8216a-17d1-4c15-b48e-11c0737b3e08" name="OutPh3" connectedTo="b519fd60-4add-411b-b4b5-f13ce3325897"/>
          <geometry xsi:type="esdl:Point" lat="36.0" lon="25.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home3-2" id="7c2a2997-8ad8-4c78-a833-8267f9444d73">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="33c1ec1d-76d0-4f77-b727-9be51356fe74" connectedTo="a6401f96-f28c-462a-9dfe-f19aa5cac0ae" name="In"/>
          <port xsi:type="esdl:OutPort" id="5935449f-1839-4dbc-baa9-cf54f15e55d8" name="Out" connectedTo="c207d0c2-4e8c-4c0d-a74e-118698d012e0"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="a79c11e3-dc63-44d6-8a54-fcde9ba759ed" connectedTo="6f962bb9-ecef-4b5f-af5f-b635ab98a869" name="In"/>
          <port xsi:type="esdl:OutPort" id="685ab748-8045-4546-9835-0293bacfab4c" name="Out" connectedTo="5be617d1-b64e-45eb-919b-d30d3480d5ff"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="245dc94a-0bbc-4130-8330-36391522687f" connectedTo="1e0ec645-7099-4e4b-98dd-8952cc4f4470" name="In"/>
          <port xsi:type="esdl:OutPort" id="a111576c-3292-4b74-a04a-b735f9fc5e56" name="Out" connectedTo="93c26271-6b8d-4a6e-87d7-88bb35b13772"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="23e5a412-4378-414e-aea2-468b70d6e4a5">
          <port xsi:type="esdl:InPort" id="c207d0c2-4e8c-4c0d-a74e-118698d012e0" name="In_Ph1" connectedTo="5935449f-1839-4dbc-baa9-cf54f15e55d8"/>
          <port xsi:type="esdl:InPort" id="5be617d1-b64e-45eb-919b-d30d3480d5ff" name="In_Ph2" connectedTo="685ab748-8045-4546-9835-0293bacfab4c"/>
          <port xsi:type="esdl:InPort" id="93c26271-6b8d-4a6e-87d7-88bb35b13772" name="In_Ph3" connectedTo="a111576c-3292-4b74-a04a-b735f9fc5e56"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home3-2" id="654b250f-657c-45f4-8ab0-7c35ee209003">
          <port xsi:type="esdl:InPort" id="27973cea-eb61-4761-945a-ad7b5d4c490f" name="In" connectedTo="3d09925a-6ac9-4148-84f2-5859f9abddd5"/>
          <port xsi:type="esdl:OutPort" id="a6401f96-f28c-462a-9dfe-f19aa5cac0ae" name="OutPh1" connectedTo="33c1ec1d-76d0-4f77-b727-9be51356fe74"/>
          <port xsi:type="esdl:OutPort" id="6f962bb9-ecef-4b5f-af5f-b635ab98a869" name="OutPh2" connectedTo="a79c11e3-dc63-44d6-8a54-fcde9ba759ed"/>
          <port xsi:type="esdl:OutPort" id="1e0ec645-7099-4e4b-98dd-8952cc4f4470" name="OutPh3" connectedTo="245dc94a-0bbc-4130-8330-36391522687f"/>
          <geometry xsi:type="esdl:Point" lat="36.0" lon="23.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable4" length="6.0" assetType="lv_line" id="1651b7de-e00b-4a2f-94ef-eb6f7899226f">
        <port xsi:type="esdl:InPort" id="ee41a942-b8b6-4d54-84b7-5dc8aa26fb2a" name="In" connectedTo="a89ac4a9-8afa-476c-a21d-4a112aedee26"/>
        <port xsi:type="esdl:OutPort" id="8449ef22-c2d9-4ce8-a38d-3ab97e2f77a2" name="Out" connectedTo="c173b2d9-469b-4b7c-80fd-a6680392467e"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid2-joint4" id="1f50a3de-5ace-4162-800f-7fd4136b904b">
        <port xsi:type="esdl:InPort" id="c173b2d9-469b-4b7c-80fd-a6680392467e" name="In" connectedTo="8449ef22-c2d9-4ce8-a38d-3ab97e2f77a2"/>
        <port xsi:type="esdl:OutPort" id="6108be56-b1f7-4200-a2ec-9c18bace99cf" name="Out" connectedTo="2c9b01fc-5940-4310-86cb-d47e0c04f9b0 149dff40-5383-4e27-a052-1c1cc845c1a8 e4df3de8-0b0e-4d32-9492-da10ac45b0ff"/>
        <geometry xsi:type="esdl:Point" lat="42.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home4-1" length="1.0" assetType="lv_line_to_home" id="da6b6770-f13b-40b1-959d-6d7b3c67289f">
        <port xsi:type="esdl:InPort" id="2c9b01fc-5940-4310-86cb-d47e0c04f9b0" name="In" connectedTo="6108be56-b1f7-4200-a2ec-9c18bace99cf"/>
        <port xsi:type="esdl:OutPort" id="5b89414d-24ed-4436-8a56-70d00669087e" name="Out" connectedTo="a10e756e-aa89-4b20-8284-c9c735de38d8"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="25.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home4-2" length="1.0" assetType="lv_line_to_home" id="bdae15e1-e3fc-476b-8428-99b060c75485">
        <port xsi:type="esdl:InPort" id="149dff40-5383-4e27-a052-1c1cc845c1a8" name="In" connectedTo="6108be56-b1f7-4200-a2ec-9c18bace99cf"/>
        <port xsi:type="esdl:OutPort" id="b1db6abe-9f80-4e76-9474-26e55a9b33a0" name="Out" connectedTo="f0005013-61fb-48e3-b9d1-ab258952639a"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="23.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home4-1" id="fdd9341a-f757-4581-ab24-066c0db00018">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="c38a42be-7a66-4051-b5dc-ee9266e6bcd7" connectedTo="c79a33e0-495a-40e0-a058-38af91b9f433" name="In"/>
          <port xsi:type="esdl:OutPort" id="9e9a69bd-351f-47ff-9a35-7de946b32fbf" name="Out" connectedTo="eabf0395-7e19-4008-80f9-a57634516952"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="8f45f2ef-15bd-4e91-bca7-f50f86d669be" connectedTo="757a2229-3428-4da1-9e8b-e5eb8195a91e" name="In"/>
          <port xsi:type="esdl:OutPort" id="ed1a81a1-9b8d-444a-bfd1-7dc4cecbeb35" name="Out" connectedTo="2f1c2af9-a74d-4462-9720-07a3a0178f07"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="6812799d-a34d-46fb-946d-cd4ad4faea44" connectedTo="c8caae5c-1508-4f8f-8b50-5e0749a2a4e4" name="In"/>
          <port xsi:type="esdl:OutPort" id="05d3fcf9-2c09-48ac-8235-1a40b8db3023" name="Out" connectedTo="b252084e-4ff3-491a-b83a-f10237cc549a"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="dc1f4584-46d5-4746-8865-7fb3d32e8f5b">
          <port xsi:type="esdl:InPort" id="eabf0395-7e19-4008-80f9-a57634516952" name="In_Ph1" connectedTo="9e9a69bd-351f-47ff-9a35-7de946b32fbf"/>
          <port xsi:type="esdl:InPort" id="2f1c2af9-a74d-4462-9720-07a3a0178f07" name="In_Ph2" connectedTo="ed1a81a1-9b8d-444a-bfd1-7dc4cecbeb35"/>
          <port xsi:type="esdl:InPort" id="b252084e-4ff3-491a-b83a-f10237cc549a" name="In_Ph3" connectedTo="05d3fcf9-2c09-48ac-8235-1a40b8db3023"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home4-1" id="e32cbc9d-184b-4025-97f3-9d5d73edfbf1">
          <port xsi:type="esdl:InPort" id="f0005013-61fb-48e3-b9d1-ab258952639a" name="In" connectedTo="b1db6abe-9f80-4e76-9474-26e55a9b33a0"/>
          <port xsi:type="esdl:OutPort" id="c79a33e0-495a-40e0-a058-38af91b9f433" name="OutPh1" connectedTo="c38a42be-7a66-4051-b5dc-ee9266e6bcd7"/>
          <port xsi:type="esdl:OutPort" id="757a2229-3428-4da1-9e8b-e5eb8195a91e" name="OutPh2" connectedTo="8f45f2ef-15bd-4e91-bca7-f50f86d669be"/>
          <port xsi:type="esdl:OutPort" id="c8caae5c-1508-4f8f-8b50-5e0749a2a4e4" name="OutPh3" connectedTo="6812799d-a34d-46fb-946d-cd4ad4faea44"/>
          <geometry xsi:type="esdl:Point" lat="42.0" lon="25.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home4-2" id="6b9eb112-4898-457a-93f1-680dd57d61f2">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="d92b1b1d-36cd-4ac5-80dc-823123d20b09" connectedTo="af44fab7-2598-471b-b709-caedadf4b56a" name="In"/>
          <port xsi:type="esdl:OutPort" id="f0caa745-8839-442b-986a-bd51d6e878cc" name="Out" connectedTo="595befa8-2dea-4f2f-b021-6c87a19a6005"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="34a03434-e7fd-4234-988e-a086a17a5ce4" connectedTo="876e8dbf-8176-4c81-a937-f46fbb331972" name="In"/>
          <port xsi:type="esdl:OutPort" id="a60315ee-8aed-4109-9885-dc21a83cbd84" name="Out" connectedTo="4f7ecc92-68e0-4026-b450-f81a5981c9be"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="bd605411-cb2d-4102-8796-28937f12615a" connectedTo="d048f91d-c1af-4a93-8414-737d8f452f6b" name="In"/>
          <port xsi:type="esdl:OutPort" id="5b372775-3d9e-459c-8d5c-6aa739147cc7" name="Out" connectedTo="24c76ab9-ef77-440a-9193-761715f8712d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="e61a97bc-22a4-4e8c-9de1-3414428ad4ae">
          <port xsi:type="esdl:InPort" id="595befa8-2dea-4f2f-b021-6c87a19a6005" name="In_Ph1" connectedTo="f0caa745-8839-442b-986a-bd51d6e878cc"/>
          <port xsi:type="esdl:InPort" id="4f7ecc92-68e0-4026-b450-f81a5981c9be" name="In_Ph2" connectedTo="a60315ee-8aed-4109-9885-dc21a83cbd84"/>
          <port xsi:type="esdl:InPort" id="24c76ab9-ef77-440a-9193-761715f8712d" name="In_Ph3" connectedTo="5b372775-3d9e-459c-8d5c-6aa739147cc7"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home4-2" id="28b399ff-51dc-47fe-8809-5e411c59c1d3">
          <port xsi:type="esdl:InPort" id="a10e756e-aa89-4b20-8284-c9c735de38d8" name="In" connectedTo="5b89414d-24ed-4436-8a56-70d00669087e"/>
          <port xsi:type="esdl:OutPort" id="af44fab7-2598-471b-b709-caedadf4b56a" name="OutPh1" connectedTo="d92b1b1d-36cd-4ac5-80dc-823123d20b09"/>
          <port xsi:type="esdl:OutPort" id="876e8dbf-8176-4c81-a937-f46fbb331972" name="OutPh2" connectedTo="34a03434-e7fd-4234-988e-a086a17a5ce4"/>
          <port xsi:type="esdl:OutPort" id="d048f91d-c1af-4a93-8414-737d8f452f6b" name="OutPh3" connectedTo="bd605411-cb2d-4102-8796-28937f12615a"/>
          <geometry xsi:type="esdl:Point" lat="42.0" lon="23.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable5" length="6.0" assetType="lv_line" id="ef0f4714-f290-44f1-9fbf-1fcaace7a190">
        <port xsi:type="esdl:InPort" id="e4df3de8-0b0e-4d32-9492-da10ac45b0ff" name="In" connectedTo="6108be56-b1f7-4200-a2ec-9c18bace99cf"/>
        <port xsi:type="esdl:OutPort" id="d5c42bff-137b-4ffe-9c0a-54e35592e0fa" name="Out" connectedTo="f5997184-9012-4404-bca7-df3df19d55fb"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="48.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid2-joint5" id="bf8427c3-1812-458a-8f84-a02e468ce344">
        <port xsi:type="esdl:InPort" id="f5997184-9012-4404-bca7-df3df19d55fb" name="In" connectedTo="d5c42bff-137b-4ffe-9c0a-54e35592e0fa"/>
        <port xsi:type="esdl:OutPort" id="edf1c4de-c475-4cd2-a97e-e3e9adead5a4" name="Out" connectedTo="99a6252e-293c-47e1-bac4-be197e912aea 39dc4f43-5196-4220-83a8-07651603bfba 59736d21-7819-4258-968f-7a35f40c6dff"/>
        <geometry xsi:type="esdl:Point" lat="48.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home5-1" length="1.0" assetType="lv_line_to_home" id="c66b68d4-992f-4faa-9156-0c4ceceec1ac">
        <port xsi:type="esdl:InPort" id="99a6252e-293c-47e1-bac4-be197e912aea" name="In" connectedTo="edf1c4de-c475-4cd2-a97e-e3e9adead5a4"/>
        <port xsi:type="esdl:OutPort" id="b9e57542-1fb2-483c-944c-9a6493c31b41" name="Out" connectedTo="5db4c8dc-77ea-4d74-8e58-85f4a7443d61"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="48.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="48.0" lon="25.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home5-2" length="1.0" assetType="lv_line_to_home" id="504d5538-ba75-4a55-96d4-0967078444b7">
        <port xsi:type="esdl:InPort" id="39dc4f43-5196-4220-83a8-07651603bfba" name="In" connectedTo="edf1c4de-c475-4cd2-a97e-e3e9adead5a4"/>
        <port xsi:type="esdl:OutPort" id="a992e6b8-63a6-4161-8021-7994ab2a495f" name="Out" connectedTo="9162791f-9270-4c85-a966-3fd8968e0aad"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="48.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="48.0" lon="23.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home5-1" id="569252c4-896a-4b40-8164-f98f06b27997">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="596c790a-f9d4-465a-872f-40160411d275" connectedTo="f1a053ec-954f-4318-a667-7830e77ad2ad" name="In"/>
          <port xsi:type="esdl:OutPort" id="932004bc-2bb2-4832-94d1-3e9c0accb337" name="Out" connectedTo="c42f0de2-0608-48c4-a436-2460b93f60ad"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="f211abbb-46bf-4392-9391-7d3b5f191437" connectedTo="da6e2073-c073-465a-b4d5-27fe442ec9c3" name="In"/>
          <port xsi:type="esdl:OutPort" id="f4442699-4639-40c3-a9d1-cf1fb21c63a5" name="Out" connectedTo="96034b93-f0bc-42c5-8234-d90a36e5f271"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="5dbde908-30e4-4556-8558-f69ca1f4dab3" connectedTo="868932d2-951e-4898-b117-b8d7038ecb80" name="In"/>
          <port xsi:type="esdl:OutPort" id="e61e20c0-6808-47c7-989e-085aad7f4225" name="Out" connectedTo="9824318c-f707-4d82-866a-80e83d45ba89"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="2795dce6-b768-4c40-80b9-b16f42be620e">
          <port xsi:type="esdl:InPort" id="c42f0de2-0608-48c4-a436-2460b93f60ad" name="In_Ph1" connectedTo="932004bc-2bb2-4832-94d1-3e9c0accb337"/>
          <port xsi:type="esdl:InPort" id="96034b93-f0bc-42c5-8234-d90a36e5f271" name="In_Ph2" connectedTo="f4442699-4639-40c3-a9d1-cf1fb21c63a5"/>
          <port xsi:type="esdl:InPort" id="9824318c-f707-4d82-866a-80e83d45ba89" name="In_Ph3" connectedTo="e61e20c0-6808-47c7-989e-085aad7f4225"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home5-1" id="c389a927-c768-42a9-acb9-b53b8d6ce7be">
          <port xsi:type="esdl:InPort" id="9162791f-9270-4c85-a966-3fd8968e0aad" name="In" connectedTo="a992e6b8-63a6-4161-8021-7994ab2a495f"/>
          <port xsi:type="esdl:OutPort" id="f1a053ec-954f-4318-a667-7830e77ad2ad" name="OutPh1" connectedTo="596c790a-f9d4-465a-872f-40160411d275"/>
          <port xsi:type="esdl:OutPort" id="da6e2073-c073-465a-b4d5-27fe442ec9c3" name="OutPh2" connectedTo="f211abbb-46bf-4392-9391-7d3b5f191437"/>
          <port xsi:type="esdl:OutPort" id="868932d2-951e-4898-b117-b8d7038ecb80" name="OutPh3" connectedTo="5dbde908-30e4-4556-8558-f69ca1f4dab3"/>
          <geometry xsi:type="esdl:Point" lat="48.0" lon="25.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home5-2" id="27faa57d-a04d-428b-b67a-03968aa87a43">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="a3edd8b9-1972-4600-ba82-e56980a4210a" connectedTo="25f6b03e-b4fc-47e0-aa67-1122cd00eeb5" name="In"/>
          <port xsi:type="esdl:OutPort" id="e7754aad-6267-4a8b-8707-e809eb169807" name="Out" connectedTo="6840f8a3-f1ed-47c0-bb0a-c3e805b95703"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="9f8bc0c9-89ec-4f78-8ce3-6804245efb19" connectedTo="da47c585-da80-4a93-9655-21a16cc59fce" name="In"/>
          <port xsi:type="esdl:OutPort" id="47b09fe7-f71e-4415-ab0a-61613e4ef897" name="Out" connectedTo="d32ab562-e81e-4eff-8568-25cd6c915e57"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="e7b739a0-210c-44c1-91d6-04260eaccf65" connectedTo="143619f1-f216-45c0-9e7c-883d58908b37" name="In"/>
          <port xsi:type="esdl:OutPort" id="4c418da7-4b5a-4dd7-9951-e0535690b414" name="Out" connectedTo="7939e56b-bc6d-44c1-a551-2fb4b758ae79"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="acb42899-028e-492a-b827-ef3f73f43049">
          <port xsi:type="esdl:InPort" id="6840f8a3-f1ed-47c0-bb0a-c3e805b95703" name="In_Ph1" connectedTo="e7754aad-6267-4a8b-8707-e809eb169807"/>
          <port xsi:type="esdl:InPort" id="d32ab562-e81e-4eff-8568-25cd6c915e57" name="In_Ph2" connectedTo="47b09fe7-f71e-4415-ab0a-61613e4ef897"/>
          <port xsi:type="esdl:InPort" id="7939e56b-bc6d-44c1-a551-2fb4b758ae79" name="In_Ph3" connectedTo="4c418da7-4b5a-4dd7-9951-e0535690b414"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home5-2" id="f77fa12b-1b6e-4919-89f7-9dd028205847">
          <port xsi:type="esdl:InPort" id="5db4c8dc-77ea-4d74-8e58-85f4a7443d61" name="In" connectedTo="b9e57542-1fb2-483c-944c-9a6493c31b41"/>
          <port xsi:type="esdl:OutPort" id="25f6b03e-b4fc-47e0-aa67-1122cd00eeb5" name="OutPh1" connectedTo="a3edd8b9-1972-4600-ba82-e56980a4210a"/>
          <port xsi:type="esdl:OutPort" id="da47c585-da80-4a93-9655-21a16cc59fce" name="OutPh2" connectedTo="9f8bc0c9-89ec-4f78-8ce3-6804245efb19"/>
          <port xsi:type="esdl:OutPort" id="143619f1-f216-45c0-9e7c-883d58908b37" name="OutPh3" connectedTo="e7b739a0-210c-44c1-91d6-04260eaccf65"/>
          <geometry xsi:type="esdl:Point" lat="48.0" lon="23.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable6" length="6.0" assetType="lv_line" id="bc9f2915-7af3-4a27-b765-0691089d4f3f">
        <port xsi:type="esdl:InPort" id="59736d21-7819-4258-968f-7a35f40c6dff" name="In" connectedTo="edf1c4de-c475-4cd2-a97e-e3e9adead5a4"/>
        <port xsi:type="esdl:OutPort" id="806697f3-73a6-4c38-a747-1deb5a1f62d3" name="Out" connectedTo="f3ec5f35-a5aa-46dd-9fd8-2f72a24f2e9d"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="48.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="54.0" lon="24.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid2-joint6" id="00ba7afa-394e-476d-9d0f-e3878e06397e">
        <port xsi:type="esdl:InPort" id="f3ec5f35-a5aa-46dd-9fd8-2f72a24f2e9d" name="In" connectedTo="806697f3-73a6-4c38-a747-1deb5a1f62d3"/>
        <port xsi:type="esdl:OutPort" id="09307dcc-6ebb-4de1-8412-f13ce197366b" name="Out" connectedTo="479f5bcb-b245-425d-a44e-b4ae984cda01 1db83429-457d-4c5d-80e5-e51cc7fc13e9"/>
        <geometry xsi:type="esdl:Point" lat="54.0" lon="24.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home6-1" length="1.0" assetType="lv_line_to_home" id="3a74375d-73a1-4d01-8af7-cbbad92691b0">
        <port xsi:type="esdl:InPort" id="479f5bcb-b245-425d-a44e-b4ae984cda01" name="In" connectedTo="09307dcc-6ebb-4de1-8412-f13ce197366b"/>
        <port xsi:type="esdl:OutPort" id="58f8713b-b2c7-4952-b0a6-80842e6647eb" name="Out" connectedTo="c68541a4-39fa-4564-b0e7-2ac8559dfc2b"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="54.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="54.0" lon="25.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid2-cable_to_home6-2" length="1.0" assetType="lv_line_to_home" id="5a5bfc7a-3239-4306-8a02-2e059ad4ad98">
        <port xsi:type="esdl:InPort" id="1db83429-457d-4c5d-80e5-e51cc7fc13e9" name="In" connectedTo="09307dcc-6ebb-4de1-8412-f13ce197366b"/>
        <port xsi:type="esdl:OutPort" id="3cce3fd8-4785-4e8a-b7b7-5c21095634f1" name="Out" connectedTo="5d7fb6e1-9306-4542-8a83-18ee02086275"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="54.0" lon="24.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="54.0" lon="23.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home6-1" id="501c600f-45e4-4252-891f-6d20e5405b65">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="2edb11da-6362-4b1c-b392-19a79de938ad" connectedTo="92429079-0179-423c-80b3-8321ad121d26" name="In"/>
          <port xsi:type="esdl:OutPort" id="dabfe24c-74e4-4b95-98cf-f8b49c947752" name="Out" connectedTo="55d3a40a-da89-477d-9e5d-3986e18ca85f"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="a1630558-8da2-4d49-b66b-c25cdc6af992" connectedTo="1c9566bf-a5b4-4f1a-87bb-240e31a2f837" name="In"/>
          <port xsi:type="esdl:OutPort" id="78e1045e-f825-4df1-a08b-d9539be88f03" name="Out" connectedTo="c2edede1-5865-409c-a5f7-84faf148f1a7"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="213c86a9-eff2-406d-97ed-8125cd8ea9f2" connectedTo="d40d612d-edb7-4e83-9d34-b2882d18f0fd" name="In"/>
          <port xsi:type="esdl:OutPort" id="b6582adf-a087-46c9-a6f4-a5f463c17a26" name="Out" connectedTo="1ab34f47-40d0-428f-b8ec-702bdfe88c13"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="bb12026f-dbb8-4033-8b4f-33a645fb98aa">
          <port xsi:type="esdl:InPort" id="55d3a40a-da89-477d-9e5d-3986e18ca85f" name="In_Ph1" connectedTo="dabfe24c-74e4-4b95-98cf-f8b49c947752"/>
          <port xsi:type="esdl:InPort" id="c2edede1-5865-409c-a5f7-84faf148f1a7" name="In_Ph2" connectedTo="78e1045e-f825-4df1-a08b-d9539be88f03"/>
          <port xsi:type="esdl:InPort" id="1ab34f47-40d0-428f-b8ec-702bdfe88c13" name="In_Ph3" connectedTo="b6582adf-a087-46c9-a6f4-a5f463c17a26"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home6-1" id="4f2b610d-8939-4dbc-8b52-be03cf896746">
          <port xsi:type="esdl:InPort" id="5d7fb6e1-9306-4542-8a83-18ee02086275" name="In" connectedTo="3cce3fd8-4785-4e8a-b7b7-5c21095634f1"/>
          <port xsi:type="esdl:OutPort" id="92429079-0179-423c-80b3-8321ad121d26" name="OutPh1" connectedTo="2edb11da-6362-4b1c-b392-19a79de938ad"/>
          <port xsi:type="esdl:OutPort" id="1c9566bf-a5b4-4f1a-87bb-240e31a2f837" name="OutPh2" connectedTo="a1630558-8da2-4d49-b66b-c25cdc6af992"/>
          <port xsi:type="esdl:OutPort" id="d40d612d-edb7-4e83-9d34-b2882d18f0fd" name="OutPh3" connectedTo="213c86a9-eff2-406d-97ed-8125cd8ea9f2"/>
          <geometry xsi:type="esdl:Point" lat="54.0" lon="25.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-2-home6-2" id="0495f32e-115a-4c32-a22a-5f15c836bcc0">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="4d0dfd82-b524-4a3a-82e3-986cd151ddcc" connectedTo="9d75d9b6-c1ba-4f5c-9404-b0a83bb6c483" name="In"/>
          <port xsi:type="esdl:OutPort" id="191c72d6-1ad1-489a-b557-3947fba179bc" name="Out" connectedTo="4928df6f-fbd9-4550-a21b-4ed17cc2ca6f"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="1ccd7804-a0c0-44aa-95de-7602d01a1b53" connectedTo="34245254-511b-495b-832f-def11c7035ba" name="In"/>
          <port xsi:type="esdl:OutPort" id="86d9acc9-7e2d-4baf-9e0b-978dc9ac95c4" name="Out" connectedTo="36b81f6a-54f2-4b3e-8d22-6d2c1891ee56"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="5e891b18-65e1-44c7-9851-c13bc71abe8f" connectedTo="ec5bf8cd-0df2-4654-ac6b-d3d7b5799f92" name="In"/>
          <port xsi:type="esdl:OutPort" id="168e3e9e-57c7-472f-87ee-f968cfb9f4ee" name="Out" connectedTo="e0dd20ec-a82a-49c0-b435-84b76c777f46"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="a4de356f-ab41-44c3-ab6a-f2c9ad6144eb">
          <port xsi:type="esdl:InPort" id="4928df6f-fbd9-4550-a21b-4ed17cc2ca6f" name="In_Ph1" connectedTo="191c72d6-1ad1-489a-b557-3947fba179bc"/>
          <port xsi:type="esdl:InPort" id="36b81f6a-54f2-4b3e-8d22-6d2c1891ee56" name="In_Ph2" connectedTo="86d9acc9-7e2d-4baf-9e0b-978dc9ac95c4"/>
          <port xsi:type="esdl:InPort" id="e0dd20ec-a82a-49c0-b435-84b76c777f46" name="In_Ph3" connectedTo="168e3e9e-57c7-472f-87ee-f968cfb9f4ee"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-2-home6-2" id="98ee1ce6-a366-489d-81b5-3f810f2a013a">
          <port xsi:type="esdl:InPort" id="c68541a4-39fa-4564-b0e7-2ac8559dfc2b" name="In" connectedTo="58f8713b-b2c7-4952-b0a6-80842e6647eb"/>
          <port xsi:type="esdl:OutPort" id="9d75d9b6-c1ba-4f5c-9404-b0a83bb6c483" name="OutPh1" connectedTo="4d0dfd82-b524-4a3a-82e3-986cd151ddcc"/>
          <port xsi:type="esdl:OutPort" id="34245254-511b-495b-832f-def11c7035ba" name="OutPh2" connectedTo="1ccd7804-a0c0-44aa-95de-7602d01a1b53"/>
          <port xsi:type="esdl:OutPort" id="ec5bf8cd-0df2-4654-ac6b-d3d7b5799f92" name="OutPh3" connectedTo="5e891b18-65e1-44c7-9851-c13bc71abe8f"/>
          <geometry xsi:type="esdl:Point" lat="54.0" lon="23.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable0" length="6.0" assetType="lv_line" id="6d9b3e8c-5a9a-45d1-a476-467a1344a63f">
        <port xsi:type="esdl:InPort" id="134d60e5-103b-4148-a464-40a01424a826" name="In" connectedTo="3b969bbe-9749-4040-8ea6-8df70ac31e2c"/>
        <port xsi:type="esdl:OutPort" id="fb47f3f3-559f-4d9c-a479-fec989ee01f2" name="Out" connectedTo="f3b8540b-e275-41de-8c80-8d541dbd87bd"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="12.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="6.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid3-joint0" id="8f80dd89-e232-456d-a0a7-3d8502c750dc">
        <port xsi:type="esdl:InPort" id="f3b8540b-e275-41de-8c80-8d541dbd87bd" name="In" connectedTo="fb47f3f3-559f-4d9c-a479-fec989ee01f2"/>
        <port xsi:type="esdl:OutPort" id="fedaec94-f7b9-47e1-a479-982e56c8e502" name="Out" connectedTo="7b7e5cc3-0717-4e85-9bda-a6ea11171629 c1f8a1e8-438c-4dfb-b7ea-7b435b33b869 810ceaa6-9d71-4664-9e99-61af4062e7a4"/>
        <geometry xsi:type="esdl:Point" lat="18.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home0-1" length="1.0" assetType="lv_line_to_home" id="5c71c2cb-6774-4e29-93d4-cf3588926f7b">
        <port xsi:type="esdl:InPort" id="7b7e5cc3-0717-4e85-9bda-a6ea11171629" name="In" connectedTo="fedaec94-f7b9-47e1-a479-982e56c8e502"/>
        <port xsi:type="esdl:OutPort" id="c1aa0660-46c1-4018-8924-f803e37c51da" name="Out" connectedTo="b5e93b2b-0d89-4968-a21e-a328f59e8c2d"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="7.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home0-2" length="1.0" assetType="lv_line_to_home" id="a5356a76-e62b-4d57-ba19-8e67c7852dfa">
        <port xsi:type="esdl:InPort" id="c1f8a1e8-438c-4dfb-b7ea-7b435b33b869" name="In" connectedTo="fedaec94-f7b9-47e1-a479-982e56c8e502"/>
        <port xsi:type="esdl:OutPort" id="07d3e3d7-c591-4556-993a-418e67dec876" name="Out" connectedTo="7da3fdf2-330e-43f8-9ada-42bb1fae292e"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="18.0" lon="5.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home0-1" id="7e7884b4-1d82-41a7-b89d-176b9b438404">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="909b02fc-bcea-4a69-acc1-e6b39e90dc78" connectedTo="441818f8-e22d-4d25-944d-7f9004219279" name="In"/>
          <port xsi:type="esdl:OutPort" id="16d7c6a9-d221-43d9-b009-b7321f0fe59b" name="Out" connectedTo="7c2f2d23-6433-4bad-aeb2-e85e302711dc"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="1900b59f-5c33-4bab-beeb-adb2e0552a08" connectedTo="e8a5025c-3fe5-4622-8521-914ab17bac3b" name="In"/>
          <port xsi:type="esdl:OutPort" id="257d6fc3-8254-4224-9b85-1e9c343b8173" name="Out" connectedTo="a282f33a-5044-4cf5-9b6e-5522ab1f0032"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="f6f444f9-e3ff-4d6c-9022-b2c7fbe2cded" connectedTo="01595e30-9116-46bd-ae25-23eb79c56101" name="In"/>
          <port xsi:type="esdl:OutPort" id="1bffdbd6-1df5-4a5a-b0d2-278690b98fb7" name="Out" connectedTo="b961d69d-7306-43bf-97ab-f3e509bbcf5d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="9ef07567-b51b-4903-b168-d172172bee86">
          <port xsi:type="esdl:InPort" id="7c2f2d23-6433-4bad-aeb2-e85e302711dc" name="In_Ph1" connectedTo="16d7c6a9-d221-43d9-b009-b7321f0fe59b"/>
          <port xsi:type="esdl:InPort" id="a282f33a-5044-4cf5-9b6e-5522ab1f0032" name="In_Ph2" connectedTo="257d6fc3-8254-4224-9b85-1e9c343b8173"/>
          <port xsi:type="esdl:InPort" id="b961d69d-7306-43bf-97ab-f3e509bbcf5d" name="In_Ph3" connectedTo="1bffdbd6-1df5-4a5a-b0d2-278690b98fb7"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home0-1" id="6e8a5fe3-5973-43d1-86b3-8abf9c29abdd">
          <port xsi:type="esdl:InPort" id="7da3fdf2-330e-43f8-9ada-42bb1fae292e" name="In" connectedTo="07d3e3d7-c591-4556-993a-418e67dec876"/>
          <port xsi:type="esdl:OutPort" id="441818f8-e22d-4d25-944d-7f9004219279" name="OutPh1" connectedTo="909b02fc-bcea-4a69-acc1-e6b39e90dc78"/>
          <port xsi:type="esdl:OutPort" id="e8a5025c-3fe5-4622-8521-914ab17bac3b" name="OutPh2" connectedTo="1900b59f-5c33-4bab-beeb-adb2e0552a08"/>
          <port xsi:type="esdl:OutPort" id="01595e30-9116-46bd-ae25-23eb79c56101" name="OutPh3" connectedTo="f6f444f9-e3ff-4d6c-9022-b2c7fbe2cded"/>
          <geometry xsi:type="esdl:Point" lat="18.0" lon="7.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home0-2" id="4072b3db-b8ed-481f-9d01-e7c52c02a2b4">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="3fe1350d-b29e-4cca-be87-ce2d222a5b63" connectedTo="2efe5be1-f0fc-4db5-9e7c-783a8d688ec8" name="In"/>
          <port xsi:type="esdl:OutPort" id="7d3c6121-7744-48c2-8c9c-07d7bf75a140" name="Out" connectedTo="9e508cb5-231c-44b7-a61d-e1fa1e4a0fc9"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="6f4081f7-7e6f-4af4-8409-bdde0ea11a94" connectedTo="1961ed86-8540-493e-abd7-d3129f2cdfc5" name="In"/>
          <port xsi:type="esdl:OutPort" id="ce041277-c4b5-48a1-baa7-bdd67fab622b" name="Out" connectedTo="7960301a-654f-43e0-a707-959a5e3fce36"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="3c5d20fc-0e37-48b2-97ec-5f0c1f84bf5b" connectedTo="f644be7c-3418-4c62-befb-db93038b0616" name="In"/>
          <port xsi:type="esdl:OutPort" id="0ee3a244-57b5-46f9-99a3-e6cc929514f1" name="Out" connectedTo="7f66deee-3e9b-446a-9300-9fd98f321f11"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="7069f749-1453-4682-9348-85efe2c7c6ff">
          <port xsi:type="esdl:InPort" id="9e508cb5-231c-44b7-a61d-e1fa1e4a0fc9" name="In_Ph1" connectedTo="7d3c6121-7744-48c2-8c9c-07d7bf75a140"/>
          <port xsi:type="esdl:InPort" id="7960301a-654f-43e0-a707-959a5e3fce36" name="In_Ph2" connectedTo="ce041277-c4b5-48a1-baa7-bdd67fab622b"/>
          <port xsi:type="esdl:InPort" id="7f66deee-3e9b-446a-9300-9fd98f321f11" name="In_Ph3" connectedTo="0ee3a244-57b5-46f9-99a3-e6cc929514f1"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home0-2" id="7a8ed9fd-37c8-4f61-ba14-42cd580876fa">
          <port xsi:type="esdl:InPort" id="b5e93b2b-0d89-4968-a21e-a328f59e8c2d" name="In" connectedTo="c1aa0660-46c1-4018-8924-f803e37c51da"/>
          <port xsi:type="esdl:OutPort" id="2efe5be1-f0fc-4db5-9e7c-783a8d688ec8" name="OutPh1" connectedTo="3fe1350d-b29e-4cca-be87-ce2d222a5b63"/>
          <port xsi:type="esdl:OutPort" id="1961ed86-8540-493e-abd7-d3129f2cdfc5" name="OutPh2" connectedTo="6f4081f7-7e6f-4af4-8409-bdde0ea11a94"/>
          <port xsi:type="esdl:OutPort" id="f644be7c-3418-4c62-befb-db93038b0616" name="OutPh3" connectedTo="3c5d20fc-0e37-48b2-97ec-5f0c1f84bf5b"/>
          <geometry xsi:type="esdl:Point" lat="18.0" lon="5.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable1" length="6.0" assetType="lv_line" id="f6aa65ce-34be-4525-98c6-a39405d503cc">
        <port xsi:type="esdl:InPort" id="810ceaa6-9d71-4664-9e99-61af4062e7a4" name="In" connectedTo="fedaec94-f7b9-47e1-a479-982e56c8e502"/>
        <port xsi:type="esdl:OutPort" id="0b1d1d93-a05c-42fe-b667-8b0fb9215d48" name="Out" connectedTo="4f6a8d05-9adf-4e1a-aa43-7db46e9ffe63"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="18.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="6.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid3-joint1" id="f6ce2a20-d137-4b66-9953-f32c03addf36">
        <port xsi:type="esdl:InPort" id="4f6a8d05-9adf-4e1a-aa43-7db46e9ffe63" name="In" connectedTo="0b1d1d93-a05c-42fe-b667-8b0fb9215d48"/>
        <port xsi:type="esdl:OutPort" id="4dc3ad31-3672-4302-9576-f1ff1750c325" name="Out" connectedTo="bebefd95-7b96-456f-a9f7-614c5e304bdb 778027af-1bfd-453c-ab21-4ff02a04cf8f 2834def5-39db-4fab-8f51-fc1a496b12ec"/>
        <geometry xsi:type="esdl:Point" lat="24.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home1-1" length="1.0" assetType="lv_line_to_home" id="6af7f0f3-f531-4249-95c1-0d349e957254">
        <port xsi:type="esdl:InPort" id="bebefd95-7b96-456f-a9f7-614c5e304bdb" name="In" connectedTo="4dc3ad31-3672-4302-9576-f1ff1750c325"/>
        <port xsi:type="esdl:OutPort" id="8a76189b-824e-4f60-b51e-6223853f05b1" name="Out" connectedTo="b4704193-478d-43f0-96d9-c97b8e67aad4"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="7.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home1-2" length="1.0" assetType="lv_line_to_home" id="ae4d6379-deed-4d26-ac5e-c14c969c2ce7">
        <port xsi:type="esdl:InPort" id="778027af-1bfd-453c-ab21-4ff02a04cf8f" name="In" connectedTo="4dc3ad31-3672-4302-9576-f1ff1750c325"/>
        <port xsi:type="esdl:OutPort" id="a212f201-6630-40c7-9b7c-219be186884b" name="Out" connectedTo="91d569dd-c447-42a4-96af-91fa4e99aa8c"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="24.0" lon="5.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home1-1" id="ba4b45b6-15e4-4221-a7ff-7742050a4528">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="13dce9f8-eb2f-4ccf-b5c8-4c239d7db626" connectedTo="69c11dde-2174-4602-ba80-0ab80ca459b6" name="In"/>
          <port xsi:type="esdl:OutPort" id="386d4aa9-bcde-4bc9-a5b8-dc0161033054" name="Out" connectedTo="da2ce639-3745-4281-8d09-c808b466bd1e"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="3ab5fc90-d3e1-430d-9730-70b2de275908" connectedTo="8985849b-f488-48d7-900d-8150be2a937d" name="In"/>
          <port xsi:type="esdl:OutPort" id="37b7fcfc-e354-4415-be5c-f62548b306ba" name="Out" connectedTo="a1372aa2-317b-44b8-9db7-a68355a84504"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="9ee01e51-0ac4-4bf1-ae68-dbd5e7a1ef9b" connectedTo="11596ee5-8013-4011-a0f0-48bdbf6e5934" name="In"/>
          <port xsi:type="esdl:OutPort" id="c7eb2586-7b1c-4792-8655-dce2c36d496e" name="Out" connectedTo="8e4733d8-454a-47fe-8370-e428203ba50e"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="3400b33a-4d7c-4798-ae76-1107b046f1b7">
          <port xsi:type="esdl:InPort" id="da2ce639-3745-4281-8d09-c808b466bd1e" name="In_Ph1" connectedTo="386d4aa9-bcde-4bc9-a5b8-dc0161033054"/>
          <port xsi:type="esdl:InPort" id="a1372aa2-317b-44b8-9db7-a68355a84504" name="In_Ph2" connectedTo="37b7fcfc-e354-4415-be5c-f62548b306ba"/>
          <port xsi:type="esdl:InPort" id="8e4733d8-454a-47fe-8370-e428203ba50e" name="In_Ph3" connectedTo="c7eb2586-7b1c-4792-8655-dce2c36d496e"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home1-1" id="d3bc485c-01e5-4a6a-a0bc-fac0eeab411c">
          <port xsi:type="esdl:InPort" id="91d569dd-c447-42a4-96af-91fa4e99aa8c" name="In" connectedTo="a212f201-6630-40c7-9b7c-219be186884b"/>
          <port xsi:type="esdl:OutPort" id="69c11dde-2174-4602-ba80-0ab80ca459b6" name="OutPh1" connectedTo="13dce9f8-eb2f-4ccf-b5c8-4c239d7db626"/>
          <port xsi:type="esdl:OutPort" id="8985849b-f488-48d7-900d-8150be2a937d" name="OutPh2" connectedTo="3ab5fc90-d3e1-430d-9730-70b2de275908"/>
          <port xsi:type="esdl:OutPort" id="11596ee5-8013-4011-a0f0-48bdbf6e5934" name="OutPh3" connectedTo="9ee01e51-0ac4-4bf1-ae68-dbd5e7a1ef9b"/>
          <geometry xsi:type="esdl:Point" lat="24.0" lon="7.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home1-2" id="0e157a60-bb66-46e2-8c02-c97ffc5b73d2">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="226e0fae-2089-44c2-953b-402794f9bf3d" connectedTo="25036522-d027-4e9f-bd50-f735dbb5ea0c" name="In"/>
          <port xsi:type="esdl:OutPort" id="1851a9f8-0cb3-47ae-8e68-96fdee7d61fb" name="Out" connectedTo="17841606-bfea-41c7-9f38-1c8d9b4aae3f"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="9a82f8d4-16b2-49af-931f-f80b03831f6c" connectedTo="fe2abeb9-03c7-403d-893d-8719bec0e19f" name="In"/>
          <port xsi:type="esdl:OutPort" id="38b4d348-4577-4b8f-92cd-94ebd385ff67" name="Out" connectedTo="8daa8322-5a54-4c5c-98e1-5a1fd383dcc7"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="a633df77-c022-40a6-b2b3-5132912c5759" connectedTo="02fc344f-ea77-406d-834b-c8d0d933054b" name="In"/>
          <port xsi:type="esdl:OutPort" id="11bad6b1-0ff1-42e5-9b93-9d1d8b71e1fc" name="Out" connectedTo="1fada39a-bfcf-4de3-bdb2-3081ff25648b"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="37d1910f-b23f-414d-97a6-3f3ce0c94f61">
          <port xsi:type="esdl:InPort" id="17841606-bfea-41c7-9f38-1c8d9b4aae3f" name="In_Ph1" connectedTo="1851a9f8-0cb3-47ae-8e68-96fdee7d61fb"/>
          <port xsi:type="esdl:InPort" id="8daa8322-5a54-4c5c-98e1-5a1fd383dcc7" name="In_Ph2" connectedTo="38b4d348-4577-4b8f-92cd-94ebd385ff67"/>
          <port xsi:type="esdl:InPort" id="1fada39a-bfcf-4de3-bdb2-3081ff25648b" name="In_Ph3" connectedTo="11bad6b1-0ff1-42e5-9b93-9d1d8b71e1fc"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home1-2" id="821590c9-059d-4822-95bc-571d8b5e44a1">
          <port xsi:type="esdl:InPort" id="b4704193-478d-43f0-96d9-c97b8e67aad4" name="In" connectedTo="8a76189b-824e-4f60-b51e-6223853f05b1"/>
          <port xsi:type="esdl:OutPort" id="25036522-d027-4e9f-bd50-f735dbb5ea0c" name="OutPh1" connectedTo="226e0fae-2089-44c2-953b-402794f9bf3d"/>
          <port xsi:type="esdl:OutPort" id="fe2abeb9-03c7-403d-893d-8719bec0e19f" name="OutPh2" connectedTo="9a82f8d4-16b2-49af-931f-f80b03831f6c"/>
          <port xsi:type="esdl:OutPort" id="02fc344f-ea77-406d-834b-c8d0d933054b" name="OutPh3" connectedTo="a633df77-c022-40a6-b2b3-5132912c5759"/>
          <geometry xsi:type="esdl:Point" lat="24.0" lon="5.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable2" length="6.0" assetType="lv_line" id="748a3602-66af-47d6-8151-e3001d8fe9e9">
        <port xsi:type="esdl:InPort" id="2834def5-39db-4fab-8f51-fc1a496b12ec" name="In" connectedTo="4dc3ad31-3672-4302-9576-f1ff1750c325"/>
        <port xsi:type="esdl:OutPort" id="09c80518-831d-4a3c-9daa-bebb5bd7ace1" name="Out" connectedTo="afe26c50-1258-43f4-8418-92588eff7726"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="24.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="6.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid3-joint2" id="07068b79-a96e-45e5-875b-760e0ad20684">
        <port xsi:type="esdl:InPort" id="afe26c50-1258-43f4-8418-92588eff7726" name="In" connectedTo="09c80518-831d-4a3c-9daa-bebb5bd7ace1"/>
        <port xsi:type="esdl:OutPort" id="b5adb3f5-97cd-4037-a403-86fc8eefe591" name="Out" connectedTo="054b7335-b1c2-42b2-aa45-3db604ceb7d3 80e7a265-2bc5-493f-8073-e98e179460b3 86c18992-5299-42e8-b616-af21bf03b52b"/>
        <geometry xsi:type="esdl:Point" lat="30.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home2-1" length="1.0" assetType="lv_line_to_home" id="f4bcbd40-4995-4782-9387-b16460a01248">
        <port xsi:type="esdl:InPort" id="054b7335-b1c2-42b2-aa45-3db604ceb7d3" name="In" connectedTo="b5adb3f5-97cd-4037-a403-86fc8eefe591"/>
        <port xsi:type="esdl:OutPort" id="e599a2bb-2cc8-4155-9b1b-e424c0ce8a01" name="Out" connectedTo="0bd6ee60-7c70-4a05-af20-49da40aadeb1"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="7.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home2-2" length="1.0" assetType="lv_line_to_home" id="9a7b02ba-b5bc-4708-89fc-2038d278f166">
        <port xsi:type="esdl:InPort" id="80e7a265-2bc5-493f-8073-e98e179460b3" name="In" connectedTo="b5adb3f5-97cd-4037-a403-86fc8eefe591"/>
        <port xsi:type="esdl:OutPort" id="c59ff24e-a27e-4786-a555-13c1dfd40373" name="Out" connectedTo="b35c1a63-9b6d-4524-bf2b-d9644a1aae1f"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="30.0" lon="5.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home2-1" id="3e017759-0489-44e7-bea3-fd3e2b963384">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="0a1db931-c815-449e-b670-3aab34844021" connectedTo="fb11c37c-2e31-4887-a9e6-a793a816ae1c" name="In"/>
          <port xsi:type="esdl:OutPort" id="4479874d-1672-4e81-96fe-94bd6f709b1a" name="Out" connectedTo="cf8e2f6d-247f-4576-bb88-d128223e27e7"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="4fcea0d9-4b06-46a2-8a8d-35bd86218292" connectedTo="b60e2191-047d-4b36-9bf2-7d508bfc01f7" name="In"/>
          <port xsi:type="esdl:OutPort" id="77f17fe7-cf13-41a3-bee5-60e74ab7dabf" name="Out" connectedTo="a2202579-085c-4c27-9b0e-4d31cc83134a"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="de404f2e-ccfd-4df7-99e6-ef2de8c95b65" connectedTo="b9c34f49-0385-4f86-b2c4-ee5ca1f3a26d" name="In"/>
          <port xsi:type="esdl:OutPort" id="c71094c1-4b28-40ed-859d-2b4031dd27c7" name="Out" connectedTo="1fe50067-a6bf-4749-9639-47651973fecb"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="b4354f3e-e753-47ac-b652-bb64e84ae102">
          <port xsi:type="esdl:InPort" id="cf8e2f6d-247f-4576-bb88-d128223e27e7" name="In_Ph1" connectedTo="4479874d-1672-4e81-96fe-94bd6f709b1a"/>
          <port xsi:type="esdl:InPort" id="a2202579-085c-4c27-9b0e-4d31cc83134a" name="In_Ph2" connectedTo="77f17fe7-cf13-41a3-bee5-60e74ab7dabf"/>
          <port xsi:type="esdl:InPort" id="1fe50067-a6bf-4749-9639-47651973fecb" name="In_Ph3" connectedTo="c71094c1-4b28-40ed-859d-2b4031dd27c7"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home2-1" id="8794f34c-ff58-4b68-8fa2-42855b326238">
          <port xsi:type="esdl:InPort" id="b35c1a63-9b6d-4524-bf2b-d9644a1aae1f" name="In" connectedTo="c59ff24e-a27e-4786-a555-13c1dfd40373"/>
          <port xsi:type="esdl:OutPort" id="fb11c37c-2e31-4887-a9e6-a793a816ae1c" name="OutPh1" connectedTo="0a1db931-c815-449e-b670-3aab34844021"/>
          <port xsi:type="esdl:OutPort" id="b60e2191-047d-4b36-9bf2-7d508bfc01f7" name="OutPh2" connectedTo="4fcea0d9-4b06-46a2-8a8d-35bd86218292"/>
          <port xsi:type="esdl:OutPort" id="b9c34f49-0385-4f86-b2c4-ee5ca1f3a26d" name="OutPh3" connectedTo="de404f2e-ccfd-4df7-99e6-ef2de8c95b65"/>
          <geometry xsi:type="esdl:Point" lat="30.0" lon="7.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home2-2" id="bd8abf05-258c-44ac-88ca-ff489f665fad">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="83105f9a-a4c4-4233-8933-6c3a124c2bed" connectedTo="22acab82-2268-44f9-a76d-059c636ec21c" name="In"/>
          <port xsi:type="esdl:OutPort" id="0900b23d-0889-4bc5-8b5f-0e3986618373" name="Out" connectedTo="20b59154-e930-4a0c-96ee-a3be0f750d8f"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="195bf68a-62a0-4a46-9e2d-093a32c46db0" connectedTo="d2145d16-1962-4981-8ca7-e73e98337785" name="In"/>
          <port xsi:type="esdl:OutPort" id="a8a95b6e-72f9-45f3-8aee-45004d1be029" name="Out" connectedTo="1e6ea6dd-a321-4568-bc3a-cc1ff62e6e7d"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="519cecf1-b042-465a-91a8-e4c399054a22" connectedTo="2a46d3f6-ca36-4234-a8a3-b26df1930ecf" name="In"/>
          <port xsi:type="esdl:OutPort" id="1e7ed04e-da71-4bbc-a50a-0c9b7b8caf2c" name="Out" connectedTo="933793d8-317f-4ac1-bdd6-ab2b8d7a50d3"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="f1a33897-fefa-4ebe-9c24-1c3fec877fb9">
          <port xsi:type="esdl:InPort" id="20b59154-e930-4a0c-96ee-a3be0f750d8f" name="In_Ph1" connectedTo="0900b23d-0889-4bc5-8b5f-0e3986618373"/>
          <port xsi:type="esdl:InPort" id="1e6ea6dd-a321-4568-bc3a-cc1ff62e6e7d" name="In_Ph2" connectedTo="a8a95b6e-72f9-45f3-8aee-45004d1be029"/>
          <port xsi:type="esdl:InPort" id="933793d8-317f-4ac1-bdd6-ab2b8d7a50d3" name="In_Ph3" connectedTo="1e7ed04e-da71-4bbc-a50a-0c9b7b8caf2c"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home2-2" id="a90d6909-08a5-436b-9a3c-7a5c3d9e3cd5">
          <port xsi:type="esdl:InPort" id="0bd6ee60-7c70-4a05-af20-49da40aadeb1" name="In" connectedTo="e599a2bb-2cc8-4155-9b1b-e424c0ce8a01"/>
          <port xsi:type="esdl:OutPort" id="22acab82-2268-44f9-a76d-059c636ec21c" name="OutPh1" connectedTo="83105f9a-a4c4-4233-8933-6c3a124c2bed"/>
          <port xsi:type="esdl:OutPort" id="d2145d16-1962-4981-8ca7-e73e98337785" name="OutPh2" connectedTo="195bf68a-62a0-4a46-9e2d-093a32c46db0"/>
          <port xsi:type="esdl:OutPort" id="2a46d3f6-ca36-4234-a8a3-b26df1930ecf" name="OutPh3" connectedTo="519cecf1-b042-465a-91a8-e4c399054a22"/>
          <geometry xsi:type="esdl:Point" lat="30.0" lon="5.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable3" length="6.0" assetType="lv_line" id="7d4d105f-3e43-4cf7-990b-c1d1e2cbedd7">
        <port xsi:type="esdl:InPort" id="86c18992-5299-42e8-b616-af21bf03b52b" name="In" connectedTo="b5adb3f5-97cd-4037-a403-86fc8eefe591"/>
        <port xsi:type="esdl:OutPort" id="bb92734e-0c2c-43fd-ba08-41b45408c2c2" name="Out" connectedTo="f7d51ef1-bcda-49ee-83b7-a11992d15b26"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="30.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="6.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid3-joint3" id="a7cd0eef-637d-4060-b647-f9114aca3882">
        <port xsi:type="esdl:InPort" id="f7d51ef1-bcda-49ee-83b7-a11992d15b26" name="In" connectedTo="bb92734e-0c2c-43fd-ba08-41b45408c2c2"/>
        <port xsi:type="esdl:OutPort" id="01c5ca81-c69d-402c-a940-6904e83a6631" name="Out" connectedTo="0ccd9ad3-1449-43d1-93b9-f0a9f9d09fc0 5c1ab468-717b-431a-bf41-c31b19915a82 651fe191-3c77-46d8-afe2-800e965f4521"/>
        <geometry xsi:type="esdl:Point" lat="36.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home3-1" length="1.0" assetType="lv_line_to_home" id="22a0f9ec-012a-4336-bf95-2f427209c445">
        <port xsi:type="esdl:InPort" id="0ccd9ad3-1449-43d1-93b9-f0a9f9d09fc0" name="In" connectedTo="01c5ca81-c69d-402c-a940-6904e83a6631"/>
        <port xsi:type="esdl:OutPort" id="8592f7da-1b06-444a-b55c-fd9125c631e1" name="Out" connectedTo="9bdfc45e-9203-4d29-b22c-461cd0c13b22"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="7.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home3-2" length="1.0" assetType="lv_line_to_home" id="af11e66b-3e35-4269-bdc4-0e2c710bed9b">
        <port xsi:type="esdl:InPort" id="5c1ab468-717b-431a-bf41-c31b19915a82" name="In" connectedTo="01c5ca81-c69d-402c-a940-6904e83a6631"/>
        <port xsi:type="esdl:OutPort" id="cf2314fe-00e9-4e0c-9abf-cb8a0d9eeaf8" name="Out" connectedTo="46327be7-f76b-44c0-8a6a-5e8d35b8503c"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="36.0" lon="5.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home3-1" id="3174d001-dcf2-4257-b3e9-90e536a5f0c6">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="641958a7-0eea-4d12-b091-d29204853733" connectedTo="bfef15ce-3e51-44fa-94b8-ba5fd31101b0" name="In"/>
          <port xsi:type="esdl:OutPort" id="10f7c197-7875-4cb7-bd7c-407410b26f0c" name="Out" connectedTo="55bff7b5-6f6f-4314-a65f-787955e1b58a"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="f2d1fa6e-a953-453f-9678-9b47ea4bcde9" connectedTo="3f81b57f-97ba-4b18-8b67-67d2ccb3cbad" name="In"/>
          <port xsi:type="esdl:OutPort" id="72cced94-f9fa-400d-ae04-0f6e39563674" name="Out" connectedTo="de4a0633-b26d-4734-a52e-ebc22eec866e"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="5ccd9c19-8dd1-4ca6-ad04-0c4804a43da8" connectedTo="6b32efa9-f4e8-4230-8dbd-91a5962a21b5" name="In"/>
          <port xsi:type="esdl:OutPort" id="5c6b0c0f-e784-438c-a17a-33541e83f329" name="Out" connectedTo="d65ed606-ebb8-4dfa-89bd-62b62e3485f1"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="9da9faae-c308-4911-abbe-c23b78a19f95">
          <port xsi:type="esdl:InPort" id="55bff7b5-6f6f-4314-a65f-787955e1b58a" name="In_Ph1" connectedTo="10f7c197-7875-4cb7-bd7c-407410b26f0c"/>
          <port xsi:type="esdl:InPort" id="de4a0633-b26d-4734-a52e-ebc22eec866e" name="In_Ph2" connectedTo="72cced94-f9fa-400d-ae04-0f6e39563674"/>
          <port xsi:type="esdl:InPort" id="d65ed606-ebb8-4dfa-89bd-62b62e3485f1" name="In_Ph3" connectedTo="5c6b0c0f-e784-438c-a17a-33541e83f329"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home3-1" id="ab9796f6-ebf0-42a0-95ca-ba14ddd425c8">
          <port xsi:type="esdl:InPort" id="46327be7-f76b-44c0-8a6a-5e8d35b8503c" name="In" connectedTo="cf2314fe-00e9-4e0c-9abf-cb8a0d9eeaf8"/>
          <port xsi:type="esdl:OutPort" id="bfef15ce-3e51-44fa-94b8-ba5fd31101b0" name="OutPh1" connectedTo="641958a7-0eea-4d12-b091-d29204853733"/>
          <port xsi:type="esdl:OutPort" id="3f81b57f-97ba-4b18-8b67-67d2ccb3cbad" name="OutPh2" connectedTo="f2d1fa6e-a953-453f-9678-9b47ea4bcde9"/>
          <port xsi:type="esdl:OutPort" id="6b32efa9-f4e8-4230-8dbd-91a5962a21b5" name="OutPh3" connectedTo="5ccd9c19-8dd1-4ca6-ad04-0c4804a43da8"/>
          <geometry xsi:type="esdl:Point" lat="36.0" lon="7.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home3-2" id="ab95b566-850b-4d75-aaac-df13ebec5a36">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="cda3887b-75ca-4478-b057-411fe6dae276" connectedTo="14d47928-32a6-45e6-ac57-cae22c137656" name="In"/>
          <port xsi:type="esdl:OutPort" id="f5ca2580-24fb-4f24-9390-6657a6becb5b" name="Out" connectedTo="6339fc71-4375-4d60-94d6-5882dfacc045"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="36da8c2f-7c9e-4306-a8ac-3e0ac7da0e13" connectedTo="943e292a-d786-49a0-8092-563746a25de3" name="In"/>
          <port xsi:type="esdl:OutPort" id="d2ffb12f-efbf-4204-8534-0363b53a2f76" name="Out" connectedTo="b2282a7a-f610-4646-ae4f-e63535a7ece8"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="4a99d381-968a-4799-8f7f-3922723d21f5" connectedTo="e0ce9fba-323c-4411-bbe6-d61d394cbb56" name="In"/>
          <port xsi:type="esdl:OutPort" id="4b6f0352-e807-4458-8643-28b01fc5cda2" name="Out" connectedTo="9a0c7878-5876-4666-8387-816885922856"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="9ae9794b-130a-4250-958a-66b05962f3f1">
          <port xsi:type="esdl:InPort" id="6339fc71-4375-4d60-94d6-5882dfacc045" name="In_Ph1" connectedTo="f5ca2580-24fb-4f24-9390-6657a6becb5b"/>
          <port xsi:type="esdl:InPort" id="b2282a7a-f610-4646-ae4f-e63535a7ece8" name="In_Ph2" connectedTo="d2ffb12f-efbf-4204-8534-0363b53a2f76"/>
          <port xsi:type="esdl:InPort" id="9a0c7878-5876-4666-8387-816885922856" name="In_Ph3" connectedTo="4b6f0352-e807-4458-8643-28b01fc5cda2"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home3-2" id="af89b345-77b5-496c-9eb1-18c1bea3c64f">
          <port xsi:type="esdl:InPort" id="9bdfc45e-9203-4d29-b22c-461cd0c13b22" name="In" connectedTo="8592f7da-1b06-444a-b55c-fd9125c631e1"/>
          <port xsi:type="esdl:OutPort" id="14d47928-32a6-45e6-ac57-cae22c137656" name="OutPh1" connectedTo="cda3887b-75ca-4478-b057-411fe6dae276"/>
          <port xsi:type="esdl:OutPort" id="943e292a-d786-49a0-8092-563746a25de3" name="OutPh2" connectedTo="36da8c2f-7c9e-4306-a8ac-3e0ac7da0e13"/>
          <port xsi:type="esdl:OutPort" id="e0ce9fba-323c-4411-bbe6-d61d394cbb56" name="OutPh3" connectedTo="4a99d381-968a-4799-8f7f-3922723d21f5"/>
          <geometry xsi:type="esdl:Point" lat="36.0" lon="5.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable4" length="6.0" assetType="lv_line" id="7021379b-4ea1-49ce-9587-61157cdcddb8">
        <port xsi:type="esdl:InPort" id="651fe191-3c77-46d8-afe2-800e965f4521" name="In" connectedTo="01c5ca81-c69d-402c-a940-6904e83a6631"/>
        <port xsi:type="esdl:OutPort" id="cebd9e49-f159-4de5-91b8-da774f0de7b0" name="Out" connectedTo="11f11a45-16bd-4ca5-a957-87e0d728a95b"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="36.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="6.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Joint" name="lv-line-grid3-joint4" id="ba8774ec-d8cc-4156-b482-0e320b113fae">
        <port xsi:type="esdl:InPort" id="11f11a45-16bd-4ca5-a957-87e0d728a95b" name="In" connectedTo="cebd9e49-f159-4de5-91b8-da774f0de7b0"/>
        <port xsi:type="esdl:OutPort" id="83e9e5ea-ede5-49d2-936c-38812645aa61" name="Out" connectedTo="046896e7-8d40-4d81-907b-48b4ec635371 aa85c54b-2d48-4058-9ae1-3305aa463c37"/>
        <geometry xsi:type="esdl:Point" lat="42.0" lon="6.0" CRS="EPSG:28992"/>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home4-1" length="1.0" assetType="lv_line_to_home" id="ebef28bb-00fa-4de7-8cc3-88214b0af94c">
        <port xsi:type="esdl:InPort" id="046896e7-8d40-4d81-907b-48b4ec635371" name="In" connectedTo="83e9e5ea-ede5-49d2-936c-38812645aa61"/>
        <port xsi:type="esdl:OutPort" id="e4071c7e-7796-48ca-9dee-91553ffd1d1d" name="Out" connectedTo="b7bfd6ba-779b-4323-aa1b-4ba8a1886c58"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="7.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:ElectricityCable" name="lv-line-grid3-cable_to_home4-2" length="1.0" assetType="lv_line_to_home" id="7dcf5251-93e3-4135-9dc5-52c30695cd80">
        <port xsi:type="esdl:InPort" id="aa85c54b-2d48-4058-9ae1-3305aa463c37" name="In" connectedTo="83e9e5ea-ede5-49d2-936c-38812645aa61"/>
        <port xsi:type="esdl:OutPort" id="3a0ad0d1-fdbe-40c6-b0da-675b4f3cfbd4" name="Out" connectedTo="c45d9bbf-f1a2-4974-81a4-02531f46f974"/>
        <geometry xsi:type="esdl:Line">
          <point xsi:type="esdl:Point" lat="42.0" lon="6.0" CRS="EPSG:28992"/>
          <point xsi:type="esdl:Point" lat="42.0" lon="5.0" CRS="EPSG:28992"/>
        </geometry>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home4-1" id="35dbf901-7a79-4a18-82c1-37c420415351">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="0e378fe6-5f60-4759-8bde-4b7fcb127c2c" connectedTo="5736dd81-65d8-4c95-ba3f-e197a272164f" name="In"/>
          <port xsi:type="esdl:OutPort" id="80e6471b-1270-435a-9052-b8f3b49568be" name="Out" connectedTo="7a209647-44f8-4730-8343-74ff65d73558"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="22e4d69d-54ec-4f6f-b93e-0ccd0c278327" connectedTo="0cf9e19a-b463-4abd-a4c7-a6586f3012d7" name="In"/>
          <port xsi:type="esdl:OutPort" id="a8c5973e-ec09-4a49-a6a7-8a9c6867a288" name="Out" connectedTo="bccb6e75-758d-4b06-8aa9-4ed6f17ae716"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="0b2c40c9-e572-4d9d-8180-e4d91115cde2" connectedTo="85240187-43d5-4acb-9729-a6a4014a5e35" name="In"/>
          <port xsi:type="esdl:OutPort" id="2881cc93-0605-4b8d-9b8a-30c0dd3a244b" name="Out" connectedTo="fbb6dcd4-3906-4c27-951b-9f8ee85ead14"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="f9ed0065-02fc-491b-921d-eddec22eaac0">
          <port xsi:type="esdl:InPort" id="7a209647-44f8-4730-8343-74ff65d73558" name="In_Ph1" connectedTo="80e6471b-1270-435a-9052-b8f3b49568be"/>
          <port xsi:type="esdl:InPort" id="bccb6e75-758d-4b06-8aa9-4ed6f17ae716" name="In_Ph2" connectedTo="a8c5973e-ec09-4a49-a6a7-8a9c6867a288"/>
          <port xsi:type="esdl:InPort" id="fbb6dcd4-3906-4c27-951b-9f8ee85ead14" name="In_Ph3" connectedTo="2881cc93-0605-4b8d-9b8a-30c0dd3a244b"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home4-1" id="77985272-7954-4f2f-b207-760f1abd0de0">
          <port xsi:type="esdl:InPort" id="c45d9bbf-f1a2-4974-81a4-02531f46f974" name="In" connectedTo="3a0ad0d1-fdbe-40c6-b0da-675b4f3cfbd4"/>
          <port xsi:type="esdl:OutPort" id="5736dd81-65d8-4c95-ba3f-e197a272164f" name="OutPh1" connectedTo="0e378fe6-5f60-4759-8bde-4b7fcb127c2c"/>
          <port xsi:type="esdl:OutPort" id="0cf9e19a-b463-4abd-a4c7-a6586f3012d7" name="OutPh2" connectedTo="22e4d69d-54ec-4f6f-b93e-0ccd0c278327"/>
          <port xsi:type="esdl:OutPort" id="85240187-43d5-4acb-9729-a6a4014a5e35" name="OutPh3" connectedTo="0b2c40c9-e572-4d9d-8180-e4d91115cde2"/>
          <geometry xsi:type="esdl:Point" lat="42.0" lon="7.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
      <asset xsi:type="esdl:Building" name="lv-grid-3-home4-2" id="fdb3a307-efa0-4c28-8ecd-835ee5ffbd75">
        <asset xsi:type="esdl:ElectricityNetwork" name="ph1">
          <port xsi:type="esdl:InPort" id="d982262a-0f4e-4f5e-acf0-7fa8ed4b6dde" connectedTo="c1324133-157d-44e7-add1-12deeb27bb51" name="In"/>
          <port xsi:type="esdl:OutPort" id="99eb7884-74c6-461a-afe2-8280ce80cbda" name="Out" connectedTo="330a8cf5-8186-455e-b3e7-7a48bfd17547"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph2">
          <port xsi:type="esdl:InPort" id="48e61f01-e2e1-410b-b168-73d54bf5fb3b" connectedTo="9795fe78-8a9b-4a0d-b348-d65aa245b9cd" name="In"/>
          <port xsi:type="esdl:OutPort" id="f897de51-932b-4e3d-9521-e49b4d96c665" name="Out" connectedTo="64a33a83-9f4c-4901-9845-fde6deecdba2"/>
        </asset>
        <asset xsi:type="esdl:ElectricityNetwork" name="ph3">
          <port xsi:type="esdl:InPort" id="f255e2fb-26b1-4d87-bd95-e80c32991411" connectedTo="a1bdd404-931e-47f0-98ce-5e7ba7ebc6ba" name="In"/>
          <port xsi:type="esdl:OutPort" id="1e4170ec-974a-4e4b-b972-480a5d1468b7" name="Out" connectedTo="b3a4b95d-e23a-49fe-902b-6f02972e1f13"/>
        </asset>
        <asset xsi:type="esdl:ElectricityDemand" name="demand" assetType="home_demand" id="5d134c7b-0978-4ee5-bf57-297c702f121f">
          <port xsi:type="esdl:InPort" id="330a8cf5-8186-455e-b3e7-7a48bfd17547" name="In_Ph1" connectedTo="99eb7884-74c6-461a-afe2-8280ce80cbda"/>
          <port xsi:type="esdl:InPort" id="64a33a83-9f4c-4901-9845-fde6deecdba2" name="In_Ph2" connectedTo="f897de51-932b-4e3d-9521-e49b4d96c665"/>
          <port xsi:type="esdl:InPort" id="b3a4b95d-e23a-49fe-902b-6f02972e1f13" name="In_Ph3" connectedTo="1e4170ec-974a-4e4b-b972-480a5d1468b7"/>
        </asset>
        <asset xsi:type="esdl:EConnection" name="lv-grid-3-home4-2" id="33a9cf27-25cd-46d0-82bb-85c395ce01ad">
          <port xsi:type="esdl:InPort" id="b7bfd6ba-779b-4323-aa1b-4ba8a1886c58" name="In" connectedTo="e4071c7e-7796-48ca-9dee-91553ffd1d1d"/>
          <port xsi:type="esdl:OutPort" id="c1324133-157d-44e7-add1-12deeb27bb51" name="OutPh1" connectedTo="d982262a-0f4e-4f5e-acf0-7fa8ed4b6dde"/>
          <port xsi:type="esdl:OutPort" id="9795fe78-8a9b-4a0d-b348-d65aa245b9cd" name="OutPh2" connectedTo="48e61f01-e2e1-410b-b168-73d54bf5fb3b"/>
          <port xsi:type="esdl:OutPort" id="a1bdd404-931e-47f0-98ce-5e7ba7ebc6ba" name="OutPh3" connectedTo="f255e2fb-26b1-4d87-bd95-e80c32991411"/>
          <geometry xsi:type="esdl:Point" lat="42.0" lon="5.0" CRS="EPSG:28992"/>
        </asset>
      </asset>
    </area>
  </instance>
</esdl:EnergySystem>
