<script setup lang="ts">
import { ref } from "vue";
import Panel from "primevue/panel";
import Select from "primevue/select";
import { useToast } from "primevue/usetoast";
import { MapLocation } from "./types.ts";

const toast = useToast();
const isPanelVisible = ref(false);

// Toggle the menu visibility
const onClick = () => {
  isPanelVisible.value = !isPanelVisible.value;
};

const props = defineProps<{
  options: MapLocation[];
  modelValue: MapLocation | null;
}>();

const emit = defineEmits<{
  (event: "update:modelValue", value: MapLocation | null): void;
  (event: "change", value: MapLocation): void;
}>();

const selectedMapLocation = ref<MapLocation | null>(props.modelValue);

// Method to handle dropdown change
const handleChange = () => {
  if (selectedMapLocation.value) {
    emit("update:modelValue", selectedMapLocation.value);
    emit("change", selectedMapLocation.value);
  }
};

// Method to get a timestamped filename
const getTimestampedFilename = (): string => {
  const now = new Date();
  const YYYmmdd = now.toISOString().slice(0, 10).replace(/-/g, "");
  const hhmmss = now.toTimeString().slice(0, 8).replace(/:/g, "");
  return `res_ffh_${YYYmmdd}_${hhmmss}.geojson`;
};

const isFetchingGeoJSON = ref<boolean>(false);
const fetchGeoJSON = async () => {
  isFetchingGeoJSON.value = true;
  try {
    // Fetch FeoJSON file
    const response = await fetch(`api/export-geojson`);
    if (!response.ok) {
      console.error("Failed to fetch GeoJSON:", response);
      toast.add({
        severity: "error",
        summary: "Error",
        detail: "Failed to fetch GeoJSON",
        life: 3000,
      });
    }
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
  <div id="menu" class="flex flex-col gap-2 flex-1">
    <Button
      @click="onClick"
      :icon="isPanelVisible ? 'pi pi-times' : 'pi pi-bars'"
      class="p-button self-end"
    />
    <Panel v-if="isPanelVisible" class="flex-none">
      <template #header>
        <div
          class="flex items-center gap-2"
          style="margin-bottom: -20px; width: 100%"
        >
          <i class="pi pi-map-marker" style="font-size: 1rem"></i>
          <span class="font-bold">Location</span>
          <Select
            v-model="selectedMapLocation"
            :options="options"
            optionLabel="label"
            placeholder="Select a location"
            class="w-full"
            @change="handleChange"
          />
        </div>
      </template>
    </Panel>
    <Panel v-if="isPanelVisible" class="flex-none">
      <template #header>
        <div
          class="flex items-center gap-2"
          style="margin-bottom: -20px; width: 100%"
        >
          <i class="pi pi-map" style="font-size: 1rem"></i>
          <span class="font-bold">Export</span>
          <Button
            type="button"
            label="GeoJSON"
            icon="pi pi-download"
            style="width: 150px"
            class="mx-auto"
            @click="fetchGeoJSON"
            :loading="isFetchingGeoJSON"
          />
        </div>
      </template>
    </Panel>
  </div>
</template>

<style scoped>
#menu {
  position: absolute;
  top: 20px;
  right: 20px;
  max-height: calc(100vh - 40px);
  width: 400px;
  z-index: 1;
}
</style>
