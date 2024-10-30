<script setup lang="ts">
import { ref, onMounted, watch } from 'vue';
// import Card from 'primevue/card';
import Panel from 'primevue/panel';
import Divider from 'primevue/divider';


import FloorHeightsMap from './FloorHeightsMap';

const map = ref();
const showAddressPoints = ref(false)
const showBuildingOutlines = ref(false)

onMounted(async () => {
  map.value = new FloorHeightsMap()
  await map.value.createMap()
});

watch(showAddressPoints, async (showAddressPoints, _) => {
  map.value.setAddressPointVisibility(showAddressPoints)
});

watch(showBuildingOutlines, async (showBuildingOutlines, _) => {
  map.value.setBuildingOutlineVisibility(showBuildingOutlines)
});


</script>

<template>
  <div id="map" class="h-full w-full"></div>
  <div id="overlay" class="flex flex-col gap-2">
    <div class="p-panel">
      <div class="flex items-center gap-2" style="padding: var(--p-panel-header-padding);">
        <!-- <Avatar icon="pi pi-home" class="mr-2" size="large" /> -->
        <i class="pi pi-home" style="font-size: 2rem"></i>
        <span class="text-3xl" style="font-weight: 200;">Floor Heights</span>
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

</style>