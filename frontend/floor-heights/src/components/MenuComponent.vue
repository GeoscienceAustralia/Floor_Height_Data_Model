<script setup lang="ts">
import Panel from "primevue/panel";
import Select from "primevue/select";
import { useToast } from "primevue/usetoast";
import { ref } from "vue";
import { MapLocation } from "./types.ts";

const toast = useToast();

const showMenu = ref(false);
const selectedMapLocation = ref<MapLocation | null>();
const isFetchingGeoJSON = ref<boolean>(false);

const props = defineProps<{
  options: MapLocation[];
}>();

const emit = defineEmits<{
  updateMapLocation: [newMapLocation: MapLocation];
}>();

const toggleMenuVisibility = () => {
  showMenu.value = !showMenu.value;
};

const emitNewMapLocation = () => {
  if (selectedMapLocation.value) {
    emit("updateMapLocation", selectedMapLocation.value);
  }
};

const getTimestampedFilename = (): string => {
  const now = new Date();
  const YYYmmdd = now.toISOString().slice(0, 10).replace(/-/g, "");
  const hhmmss = now.toTimeString().slice(0, 8).replace(/:/g, "");
  return `res_ffh_${YYYmmdd}_${hhmmss}.geojson`;
};

const fetchGeoJSON = async () => {
  isFetchingGeoJSON.value = true;
  try {
    // Fetch GeoJSON file
    const response = await fetch(`api/geojson/`);
    const geojson = await response.json();
    const blob = new Blob([JSON.stringify(geojson)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);

    // Generate the timestamped filename
    const filename = getTimestampedFilename();

    // Create a link and trigger download
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();

    // Revoke object URL
    link.remove();
    URL.revokeObjectURL(url);
  } catch (error) {
    console.error("Failed to fetch GeoJSON:", error);
    toast.add({
      severity: "error",
      summary: "Error",
      detail: "Failed to fetch GeoJSON",
      life: 3000,
    });
  } finally {
    isFetchingGeoJSON.value = false;
  }
};
</script>

<template>
  <div id="menu" class="flex flex-row gap-2 flex-1">
    <div v-if="showMenu" id="overlay" class="flex flex-col gap-2 flex-1">
      <Panel class="flex-none">
        <template #header>
          <div class="flex items-center gap-2" style="margin-bottom: -20px; width: 100%">
            <i class="pi pi-map-marker" style="font-size: 1rem"></i>
            <span class="font-bold">Location</span>
            <Select v-model="selectedMapLocation" :options="props.options" optionLabel="label"
              placeholder="Select a location" class="w-full" @change="emitNewMapLocation" />
          </div>
        </template>
      </Panel>
      <Panel v-if="showMenu" class="flex-none">
        <template #header>
          <div class="flex items-center gap-2" style="margin-bottom: -20px; width: 100%">
            <i class="pi pi-map" style="font-size: 1rem"></i>
            <span class="font-bold">Export</span>
            <Button type="button" label="GeoJSON" icon="pi pi-download" style="width: 150px" class="mx-auto"
              @click="fetchGeoJSON" :loading="isFetchingGeoJSON" />
          </div>
        </template>
      </Panel>
    </div>
    <div class="flex-none">
      <Button @click="toggleMenuVisibility" :icon="showMenu ? 'pi pi-times' : 'pi pi-bars'"
        size="small" class="button"/>
    </div>
  </div>
</template>

<style scoped>
#menu {
  position: absolute;
  top: 20px;
  right: 10px;
  z-index: 1;
}

#overlay {
  width: 400px;
  z-index: 1;
}

.button{
  width: 2rem;
  height: 2rem;
}
</style>
