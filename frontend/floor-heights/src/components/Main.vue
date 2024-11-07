<script setup lang="ts">
import axios from 'axios';
import { ref, onMounted, watch } from 'vue';
import Panel from 'primevue/panel';
import { useToast } from "primevue/usetoast";
import FloorHeightsMap from './FloorHeightsMap';
import {FloorMeasure, AddressPoint, Building} from './types';
import FloorMeasureComponent from './FloorMeasureComponent.vue';

const toast = useToast();

const map = ref();
const showAddressPoints = ref(false);
const showBuildingOutlines = ref(false);

const clickedAddressPoint = ref<AddressPoint | null>(null);
const clickedBuilding = ref<Building | null>(null);

const clickedFloorMeasures = ref<FloorMeasure[]>([]);

onMounted(async () => {
  clickedAddressPoint.value = null;
  clickedBuilding.value = null;

  map.value = new FloorHeightsMap();
  await map.value.createMap();

  map.value.emitter.on('addressPointClicked', onAddressPointClicked);
  map.value.emitter.on('buildingClicked', onBuildingClicked);

  showAddressPoints.value = true;
  showBuildingOutlines.value = true;
});

watch(showAddressPoints, async (showAddressPoints, _) => {
  map.value.setAddressPointVisibility(showAddressPoints);
});

watch(showBuildingOutlines, async (showBuildingOutlines, _) => {
  map.value.setBuildingOutlineVisibility(showBuildingOutlines);
});

const onAddressPointClicked = (clickedObject: any) => {
  console.log('Address Point clicked:', clickedObject);
  clickedAddressPoint.value = clickedObject;
  clickedBuilding.value = null;
};

const onBuildingClicked = (clickedObject: any) => {
  console.log('Building clicked:', clickedObject);
  clickedBuilding.value = clickedObject;
  clickedAddressPoint.value = null;

  if (!clickedBuilding.value) return;
  fetchFloorMeasures(clickedBuilding.value?.id);
};

const fetchFloorMeasures = async (building_id: string) => {
  try {
    const response = await axios.get<FloorMeasure[]>(`api/floor-height-data/${building_id}`);
    clickedFloorMeasures.value = response.data
    console.log(clickedFloorMeasures.value);
  } catch (error) {
    console.error(`Failed to fetch floor measures for building id ${building_id}`);
    toast.add(
      {
        severity: 'error',
        summary: 'Error',
        detail: 'Failed to fetch floor measure data',
        life: 3000
      }
    );
  }
}

</script>

<template>
  <div id="map" class="h-full w-full"></div>
  <div id="overlay" class="flex flex-col gap-2">
    <Toast />
    <div class="p-panel" style="background-color: var(--p-primary-color);">
      <div class="flex items-center gap-2" style="padding: var(--p-panel-header-padding);">
        <i class="pi pi-home" style="font-size: 2rem; color: white;"></i>
        <span class="text-3xl title">Floor Heights</span>
      </div>
    </div>

    <Panel>
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <!-- <Avatar icon="pi pi-clone" /> -->
          <i class="pi pi-clone" style="font-size: 1rem"></i>
          <span class="font-bold">Layers</span>
        </div>
      </template>
      <div class="flex flex-col gap-1">
        <div>
          Select layers to show in map
        </div>
        <div class="flex items-center pl-2">
          <Checkbox inputId="showAddressPoints" v-model="showAddressPoints" :binary="true" />
          <label for="showAddressPoints" class="ml-2"> Address Points </label>
        </div>
        <div class="flex items-center pl-2">
          <Checkbox inputId="showBuildingOutlines" v-model="showBuildingOutlines" :binary="true" />
          <label for="showBuildingOutlines" class="ml-2"> Building Outlines </label>
        </div>
      </div>      
    </Panel>

    <Panel v-if="clickedAddressPoint">
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <i class="pi pi-map-marker" style="font-size: 1rem"></i>
          <span class="font-bold">Address Point</span>
        </div>
      </template>
      <div class="flex flex-col gap-1">
        <div class="flex flex-col gap-0">
          <div class="subheading"> Address: </div>
          <div> {{ clickedAddressPoint.address }} </div>
        </div>
        <div class="flex flex-col gap-0">
          <div class="subheading"> GNAF ID: </div>
          <div> {{ clickedAddressPoint.gnaf_id }} </div>
        </div>
      </div>
      
    </Panel>

    <Panel v-if="clickedBuilding">
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <i class="pi pi-building" style="font-size: 1rem"></i>
          <span class="font-bold">Building</span>
        </div>
      </template>
      <div class="flex flex-col gap-1">
        <div class="flex flex-col gap-0">
          <div class="subheading"> Absolute Height (m): </div>
          <div> {{ clickedBuilding.height_ahd }} </div>
        </div>
      </div>
    </Panel>

    <Panel v-if="!(clickedAddressPoint || clickedBuilding)" >
      <div class="flex flex-col gap-2">
        <div class="flex flex-row w-full items-center justify-center content-center gap-2">
          <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
        </div>
        <div class="flex flex-col gap-1">
          <div class="opacity-50"> Select address point or building outline to show floor height data. </div>
        </div>
      </div>
    </Panel>

    <Panel v-if="clickedBuilding && clickedFloorMeasures.length == 0" >
      <div class="flex flex-col gap-2">
        <div class="flex flex-row w-full items-center justify-center content-center gap-2">
          <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
        </div>
        <div class="flex flex-col gap-1">
          <div class="opacity-50"> No floor measures found for this building </div>
        </div>
      </div>
    </Panel>

    <Panel v-if="clickedBuilding && clickedFloorMeasures.length != 0" >
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <i class="pi pi-chart-scatter" style="font-size: 1rem"></i>
          <span class="font-bold">Measures</span>
        </div>
      </template>
      <div class="flex flex-col gap-1">
        <FloorMeasureComponent
          v-for="floorMeasure in clickedFloorMeasures"
          :key="floorMeasure.id"
          :floor-measure="floorMeasure"
        />
      </div>
    </Panel>

  </div>
  
  
  
</template>

<style scoped>

#map {
  height: 100vh;
}

#overlay {
  position: absolute;
  top: 20px;
  left: 20px;
  bottom: 20px;
  width: 400px;
  z-index: 1; /* Ensures it stays above the map */
}

.subheading {
  color:var(--p-primary-500);
  font-size: 0.9em;
  margin-bottom: -4px;
}

.title {
  font-weight: 700;
  color: white;
  letter-spacing: -2px;
}

</style>