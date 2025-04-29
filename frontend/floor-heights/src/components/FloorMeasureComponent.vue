<script setup lang="ts">
import type { PropType } from 'vue';
import Divider from 'primevue/divider';
import type { FloorMeasure } from './types';

defineProps({
  floorMeasure: Object as PropType<FloorMeasure>,
  isLast: {
    type: Boolean,
    default: false,
  },
});

</script>

<template>
  <div class="flex flex-col gap-1">
    <div class="flex flex-col gap-0">
      <div class="subheading"> Dataset: </div>
      <div> {{ floorMeasure?.datasets.toString() }} </div>
    </div>
    <div class="flex flex-col gap-0">
      <div class="subheading"> Method: </div>
      <div> {{ floorMeasure?.method }} </div>
    </div>
    <div class="flex flex-col gap-0">
      <div class="subheading"> Height (m): </div>
      <div> {{ floorMeasure?.height.toFixed(3) }} </div>
    </div>
    <div class="flex flex-col gap-0">
      <div class="subheading"> Accuracy: </div>
      <div> {{ floorMeasure?.accuracy_measure }} </div>
    </div>
    <Divider v-if="!isLast" />
  </div>
</template>

<style scoped>
.subheading {
  color: var(--p-primary-500);
  font-size: 0.9em;
  margin-bottom: -4px;
}
</style>