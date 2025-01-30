<script setup lang="ts">
import { defineProps, defineEmits, ref } from "vue";
import Panel from "primevue/panel";
import Select from "primevue/select";
import { MapLocation } from "./types.ts";

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
