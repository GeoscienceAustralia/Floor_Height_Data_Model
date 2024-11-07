<script setup lang="ts">
import { defineProps } from 'vue';
import type { PropType } from 'vue'
import Divider from 'primevue/divider';
import type { FloorMeasure} from './types';

const props = defineProps({
  floorMeasure: Object as PropType<FloorMeasure>
});

</script>

<template>
  <div class="flex flex-col gap-1">
    <div class="flex flex-col gap-0">
      <div class="subheading"> Method: </div>
      <div> {{ floorMeasure?.method }} </div>
    </div>
    <div class="flex flex-col gap-0">
      <div class="subheading"> Height (m): </div>
      <div> {{ floorMeasure?.height }} </div>
    </div>
    <div class="flex flex-col gap-0">
      <div class="subheading"> Accuracy: </div>
      <div> {{ floorMeasure?.accuracy_measure }} </div>
    </div>
    <Divider />
  </div>
</template>

<style scoped>

.subheading {
  color:var(--p-primary-500);
  font-size: 0.9em;
  margin-bottom: -4px;
}

</style>