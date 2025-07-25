<script setup lang="ts">
import axios from 'axios';
import { LngLatBoundsLike, LngLat } from 'maplibre-gl';
import { ref, onMounted, watch, computed } from 'vue';
import MultiSelect from 'primevue/multiselect';
import Panel from 'primevue/panel';
import ScrollPanel from 'primevue/scrollpanel';
import { useToast } from "primevue/usetoast";
import FloorHeightsMap from './FloorHeightsMap';
import {FloorMeasure, AddressPoint, Building, MapLocation, GraduatedFillLegend} from './types';
import FloorMeasureComponent from './FloorMeasureComponent.vue';
import MenuComponent from './MenuComponent.vue';
import ImageWindowComponent from './ImageWindowComponent.vue';
import LegendComponent from './LegendComponent.vue';

const toast = useToast();

const map = ref();
const showLegend = ref(false);
const showImageWindow = ref(false);
const showAddressPoints = ref(false);
const showBuildingOutlines = ref(false);
const showBuildingOutlineOptions = ref(false);
const buildingOutlineMethodFilterOptions = ref<String[]>([]);
const buildingOutlineMethodFilterSelection = ref<String[]>([]);
const buildingOutlineDatasetFilterOptions = ref<String[]>([]);
const buildingOutlineDatasetFilterSelection = ref<String[]>([]);
const buildingOutlineFillSelection = ref<String | null>(null);

const clickedAddressPoint = ref<AddressPoint | null>(null);
const clickedBuilding = ref<Building | null>(null);
const clickedFloorMeasures = ref<FloorMeasure[]>([]);

const legendType = ref<String | null>(null);
const legendObject = ref<Record<string, string>>({});
const MAX_NUM_LEGEND_ITEMS: number = 20; // We only have 20 colours to choose from
const buildingGraduatedFillLegend = ref<GraduatedFillLegend | null>(null);
const buildingCategorisedFillLegend = ref<String[]>([]);
const legendState = computed(() => {
  if (!showBuildingOutlines.value) return null;

  if (buildingOutlineFillSelection.value === "Floor Height") {
    if (buildingGraduatedFillLegend.value?.min == null || buildingGraduatedFillLegend.value?.max == null) {
      return { state: "no-data", message: "No data found for the selected filters." };
    }
  } else if (buildingOutlineFillSelection.value == "Dataset" || buildingOutlineFillSelection.value == "Method") {
    if (buildingCategorisedFillLegend.value.length == 0) {
      return { state: "no-data", message: "No data found for the selected filters." };
    }
    if (buildingCategorisedFillLegend.value.length > MAX_NUM_LEGEND_ITEMS) {
      return { state: "too-many-items", message: "Too many items, try filtering by datasets or methods." };
    }
  }

  return { state: "valid" };
});

// Define options for the fill dropdown
const buildingOutlineFillOptions: string[] = [
  'Floor Height',
  'Dataset',
  'Method',
];

// Define locations for the menu dropdown
const mapLocationOptions: MapLocation[] = [
  { label: 'Wagga Wagga, NSW', coordinates: new LngLat(147.370, -35.120) },
  { label: 'Launceston, TAS', coordinates: new LngLat(147.144, -41.422) },
  { label: 'Tweed Heads, NSW', coordinates: new LngLat(153.537, -28.205) },
];

const selectedMapLocation = ref<MapLocation>(mapLocationOptions[0]);

onMounted(async () => {
  clickedAddressPoint.value = null;
  clickedBuilding.value = null;

  map.value = new FloorHeightsMap();
  await map.value.createMap(selectedMapLocation.value.coordinates);

  try {
    buildingOutlineMethodFilterOptions.value = (await axios.get<String[]>(`api/methods/`)).data;
    buildingOutlineDatasetFilterOptions.value = (await axios.get<String[]>(`api/datasets/`)).data;
  } catch (error) {
    console.error(`Failed to fetch filter options`);
    toast.add(
      {
        severity: 'error',
        summary: 'Error',
        detail: 'Failed to fetch filter options',
        life: 3000
      }
    );
  }

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
  if (!showBuildingOutlines) {
    showLegend.value = false;
  }
});

watch(buildingOutlineMethodFilterSelection, (newFilter) => {
  if (newFilter == null) {
    resetMethodFilters();
  }
});

watch(buildingOutlineDatasetFilterSelection, (newFilter) => {
  if (newFilter == null) {
    resetDatasetFilters();
  }
});

watch([showBuildingOutlines, buildingOutlineDatasetFilterSelection, buildingOutlineMethodFilterSelection], async ([showBuildingOutlines, datasets, methods]) => {
  if (showBuildingOutlines) {
    map.value.setBuildingFilter(datasets, methods);
  }
});

watch([buildingOutlineFillSelection], ([newSelection]) => {
  if (newSelection == null) {
    map.value.resetBuildingTiles();
    showLegend.value = false;
  }
});

watch([buildingOutlineMethodFilterSelection, buildingOutlineDatasetFilterSelection, buildingOutlineFillSelection, selectedMapLocation], async ([methods, datasets, fillOption, selectedMapLocation]) => {
  // Sort so that the dropdown options match the API request
  methods.sort()
  datasets.sort()

  // If a method or dataset filter isn't applied, set to all values
  methods = methods.length ? methods : buildingOutlineMethodFilterOptions.value;
  datasets = datasets.length ? datasets : buildingOutlineDatasetFilterOptions.value;

  const locationBounds = generateLocationBounds(selectedMapLocation)

  if (fillOption === 'Floor Height') {
    await setGraduatedFill(methods, datasets, locationBounds);
  }
  if (fillOption === 'Dataset' || fillOption === 'Method') {
    await setCategorisedFill(methods, datasets, locationBounds, fillOption);
  }
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

const fetchFloorMeasures = async (buildingId: string) => {
  try {
    const response = await axios.get<FloorMeasure[]>(`api/floor-height-data/${buildingId}`);
    clickedFloorMeasures.value = response.data
  } catch (error) {
    console.error(`Failed to fetch floor measures for building id ${buildingId}`);
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

const fetchGraduatedLegendValues = async (methods: String[], datasets: String[], locationBounds: LngLatBoundsLike) => {
  let queryParams: Record<string, string> = {
    method_filter: methods.toString(),
    dataset_filter: datasets.toString(),
    bbox: locationBounds.toString()
  };

  try {
    buildingGraduatedFillLegend.value = (await axios.get<GraduatedFillLegend>(`api/legend-graduated-values/?${new URLSearchParams(queryParams)}`)).data;
  } catch (error) {
    console.error(`Failed to fetch legend values`);
    toast.add({
      severity: "error",
      summary: "Error",
      detail: "Failed to fetch legend values",
      life: 3000,
    });
  }
};

const fetchCategorisedLegendValues = async (table: string, methods: String[], datasets: String[], locationBounds: LngLatBoundsLike) => {
  let queryParams: Record<string, string> = {
    method_filter: methods.toString(),
    dataset_filter: datasets.toString(),
    bbox: locationBounds.toString()
  };

  try {
    buildingCategorisedFillLegend.value = (await axios.get<String[]>(`api/legend-categorised-values/${table}?${new URLSearchParams(queryParams)}`)).data;
  } catch (error) {
    console.error(`Failed to fetch legend values`);
    toast.add({
      severity: "error",
      summary: "Error",
      detail: "Failed to fetch legend values",
      life: 3000,
    });
  }
};

const createCategorisedLegendObject = (colorMap: (number | string)[]) => {
  const legend: Record<string, string> = {};

  for (let i = 0; i < colorMap.length - 1; i += 2) {
    const label = colorMap[i] as number;
    const color = colorMap[i + 1]  as string;
    legend[label] = color;
  }

  legendObject.value = legend;
};

const createGraduatedLegendObject = (colorMap: (number | string | null)[]) => {
  const legend: Record<number, string> = {};

  for (let i = 0; i < colorMap.length - 1; i += 2) {
    const label = colorMap[i] as number | null;
    const color = colorMap[i + 1] as string;

    // If a null label is found, clear the legend and return
    if (label === null) {
      legendObject.value = {};
      return;
    }

    legend[label] = color;
  }

  legendObject.value = legend;
};

const setGraduatedFill = async (methods: String[], datasets: String[], locationBounds: LngLatBoundsLike) => {
  await fetchGraduatedLegendValues(methods, datasets, locationBounds);
  // Check for an empty legend
  if (buildingGraduatedFillLegend.value?.min == null || buildingGraduatedFillLegend.value?.max == null) {
    map.value.resetBuildingTiles();
    showLegend.value = true;
    return;
  }
  const colorMap = map.value.generateGraduatedColorMap(buildingGraduatedFillLegend.value?.min, buildingGraduatedFillLegend.value?.max)
  map.value.setBuildingFloorHeightGraduatedFill(methods, datasets, colorMap, locationBounds);
  createGraduatedLegendObject(colorMap)
  legendType.value = "graduated";
  showLegend.value = true;
};

const setCategorisedFill = async (methods: String[], datasets: String[], locationBounds: LngLatBoundsLike, fillOption: String) => {
  const table = fillOption.toLowerCase() === 'dataset' ? 'dataset_names' : 'method_names';
  await fetchCategorisedLegendValues(fillOption.toLowerCase(), methods, datasets, locationBounds);
  // Check for an empty legend
  if (buildingCategorisedFillLegend.value.length === 0) {
    map.value.resetBuildingTiles();
    showLegend.value = true;
    return;
  }
  // Check if the number of legend items exceed the maximum
  if (buildingCategorisedFillLegend.value.length > MAX_NUM_LEGEND_ITEMS) {
    map.value.resetBuildingTiles();
    showLegend.value = true;
    return;
  }
  const colorMap = map.value.generateCategorisedColorMap(buildingCategorisedFillLegend.value)
  map.value.setBuildingCategorisedFill(methods, datasets, colorMap, table, locationBounds);
  createCategorisedLegendObject(colorMap)
  legendType.value = "categorised";
  showLegend.value = true;
};

const generateLocationBounds = (location: MapLocation) => {
  // Roughly generate a bounding box around the location
  const locationBounds: LngLatBoundsLike = [
    [location.coordinates.lng - 0.7, location.coordinates.lat - 0.7],
    [location.coordinates.lng + 0.7, location.coordinates.lat + 0.7]
  ];
  return locationBounds;
};

const updateMapLocation = (newLocation: MapLocation) => {
  if (map) {
    map.value.setCenter(newLocation.coordinates);
    map.value.setZoom(13);
    selectedMapLocation.value = newLocation;
  }
};

const resetMethodFilters = () => {
  buildingOutlineMethodFilterSelection.value = []
}

const resetDatasetFilters = () => {
  buildingOutlineDatasetFilterSelection.value = []
}

const filteredFloorMeasures = computed(() => {
  // If no filters are selected, return all floor measures
  if (
    !buildingOutlineMethodFilterSelection.value.length &&
    !buildingOutlineDatasetFilterSelection.value.length
  ) {
    return clickedFloorMeasures.value;
  }

  // Filter floor measures based on the selected methods and datasets
  return clickedFloorMeasures.value.filter((measure) => {
    const matchesMethod =
      !buildingOutlineMethodFilterSelection.value.length ||
      buildingOutlineMethodFilterSelection.value.includes(measure.method);

    const matchesDataset =
      !buildingOutlineDatasetFilterSelection.value.length ||
      buildingOutlineDatasetFilterSelection.value.some((selectedDataset) =>
        measure.datasets.includes(selectedDataset.toString())
      );

    return matchesMethod && matchesDataset;
  });
});
</script>

<template>
  <Toast />
  <div id="map" class="h-full w-full"></div>
  <div id="overlay" class="flex flex-col gap-2 flex-1 ">
    <div class="flex-none p-panel" style="background-color: var(--p-primary-color);">
      <div class="flex items-center gap-2" style="padding: var(--p-panel-header-padding);">
        <i class="pi pi-home" style="font-size: 2rem; color: white;"></i>
        <span class="text-3xl title">Floor Heights</span>
      </div>
    </div>

    <Panel class="flex-none">
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <!-- <Avatar icon="pi pi-clone" /> -->
          <i class="pi pi-clone" style="font-size: 1rem"></i>
          <span class="font-bold">Layers</span>
        </div>
      </template>
      <div class="flex flex-col gap-1">
        <div class="flex items-center pl-2">
          <div class="items-center">
            <Checkbox inputId="showAddressPoints" v-model="showAddressPoints" :binary="true" />
            <label for="showAddressPoints" class="ml-2">Address Points</label>
          </div>
        </div>
        <div class="flex pl-2 items-center justify-between">
          <div class="items-center">
            <Checkbox inputId="showBuildingOutlines" v-model="showBuildingOutlines" :binary="true" />
            <label for="showBuildingOutlines" class="ml-2">Building Outlines</label>
          </div>
          <ToggleButton
            v-model="showBuildingOutlineOptions"
            onIcon="pi pi-angle-up"
            offIcon="pi pi-cog"
            :pt="{
              label: (_) => ({
                style: {
                  'width': 0,
                  'height': 0,
                  'visibility': 'hidden'
                },
              }),
              root: (_) => ({
                style: {
                  'background-color':'unset',
                  'border':'unset'
                },
              })
            }"
          />
        </div>
        <div v-if="showBuildingOutlineOptions" class="flex items-center justify-between gap-2 pl-6">
          <i class="pi pi-filter" style="font-size: 1rem"></i>
          <MultiSelect
            v-model="buildingOutlineDatasetFilterSelection"
            :options="buildingOutlineDatasetFilterOptions"
            placeholder="Filter Datasets"
            class="w-full min-w-0"
            @change="resetMethodFilters"
            showClear
            />
        </div>
        <div v-if="showBuildingOutlineOptions" class="flex items-center justify-between gap-2 pl-6">
          <i class="pi pi-filter" style="font-size: 1rem"></i>
          <MultiSelect
            v-model="buildingOutlineMethodFilterSelection"
            :options="buildingOutlineMethodFilterOptions"
            placeholder="Filter Methods"
            class="w-full min-w-0"
            @change="resetDatasetFilters"
            showClear
            />
        </div>
        <div v-if="showBuildingOutlineOptions" class="flex items-center justify-between gap-2 pl-6">
          <i class="pi pi-palette" style="font-size: 1rem"></i>
            <Select
              v-model="buildingOutlineFillSelection"
              :options="buildingOutlineFillOptions"
              placeholder="Fill Variable"
              class="w-full min-w-0"
              showClear
              />
        </div>
        <div v-if="showBuildingOutlineOptions" class="flex items-center justify-between gap-2 pl-6">
          <div class="items-center">
            <i class="pi pi-image" style="font-size: 1rem"></i>
            <label for="showImageWindow" class="ml-2">Show Image Window</label>
          </div>
          <ToggleSwitch v-model="showImageWindow" />
        </div>
      </div>
    </Panel>

    <Panel class="flex-none" v-if="clickedAddressPoint">
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <i class="pi pi-map-marker" style="font-size: 1rem"></i>
          <span class="font-bold">Address Point</span>
        </div>
      </template>
      <div class="flex flex-col gap-1">
        <div class="flex flex-col gap-0">
          <div class="subheading">Address:</div>
          <div> {{ clickedAddressPoint.address }} </div>
        </div>
        <div class="flex flex-col gap-0">
          <div class="subheading">GNAF ID:</div>
          <div> {{ clickedAddressPoint.gnaf_id }} </div>
        </div>
      </div>
    </Panel>

    <Panel class="flex-none" v-if="clickedBuilding">
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <i class="pi pi-building" style="font-size: 1rem"></i>
          <span class="font-bold">Building</span>
        </div>
      </template>
      <div class="flex flex-col gap-1">
        <div class="flex flex-col gap-0">
          <div class="subheading">Absolute Height (m)</div>
          <div class="flex flex-row w-full">
            <div class="basis-1/2"> min: {{ clickedBuilding.min_height_ahd.toFixed(3) }} </div>
            <div class="basis-1/2"> max: {{ clickedBuilding.max_height_ahd.toFixed(3) }} </div>
          </div>
        </div>
      </div>
    </Panel>

    <Panel class="flex-none" v-if="!(clickedAddressPoint || clickedBuilding)" >
      <div class="flex flex-col items-center justify-center gap-2">
        <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
        <div class="opacity-50"> Select address point or building outline to show floor height data. </div>
      </div>
    </Panel>

    <Panel class="flex-none text-center items-center" v-if="clickedBuilding && clickedFloorMeasures.length == 0" >
      <div class="flex flex-col gap-2">
        <div class="flex flex-col items-center justify-center gap-2">
          <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
        </div>
        <div class="opacity-50">No floor measures found for this building.</div>
      </div>
    </Panel>

    <Panel
      class="flex shrink flex-col min-h-0"
      v-if="clickedBuilding && clickedFloorMeasures.length != 0"
      :pt="{
        contentContainer: (_) => ({
            id: 'myPanelHeader',
            class: [
                'flex-1',
                'flex',
                'flex-col',
                'min-h-0'
            ]
        }),
        content: (_) => ({
            id: 'myPanelContent',
            class: [
                'flex-1',
                'flex',
                'flex-col',
                'min-h-0'
            ]
        })
      }"
    >
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <i class="pi pi-chart-scatter" style="font-size: 1rem"></i>
          <span class="font-bold">Measures</span>
        </div>
      </template>

      <div class="flex flex-1 flex-col min-h-0">
        <div class="flex max-h-full min-h-0">
          <ScrollPanel class="flex-1 max-h-full w-full" style="height: unset;">
          <div>
            <template v-if="filteredFloorMeasures.length > 0">
              <FloorMeasureComponent
                v-for="(fm, index) in filteredFloorMeasures"
                :key="fm.id"
                :floorMeasure="fm"
                :isLast="index === filteredFloorMeasures.length - 1"
              />
            </template>
            <template v-else>
              <div class="flex flex-col items-center justify-center gap-2">
                <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
                <div class="opacity-50">No floor measures found for the selected filters.</div>
              </div>
            </template>
          </div>
        </ScrollPanel>
        </div>
      </div>
    </Panel>
  </div>

  <MenuComponent v-model="selectedMapLocation" :options="mapLocationOptions" @update-map-location="updateMapLocation"/>
  <ImageWindowComponent v-if="showImageWindow" :building="clickedBuilding" @close-image-window="showImageWindow = !showImageWindow"/>
  <LegendComponent v-if="showLegend" :legendType="legendType" :legendObject="legendObject" :fillOption="buildingOutlineFillSelection" :legendState="legendState"/>
</template>

<style scoped>
:deep(.p-multiselect-clear-icon) {
  background-color: var(--p-surface-0);
  margin-right: -8px;
}

:deep(.p-select-clear-icon) {
  background-color: var(--p-surface-0);
  margin-right: -8px;
}

#map {
  height: 100vh;
}

#overlay {
  position: absolute;
  top: 20px;
  left: 20px;
  max-height: calc(100vh - 40px);
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