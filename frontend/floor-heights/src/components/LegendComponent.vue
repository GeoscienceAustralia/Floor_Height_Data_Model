<script setup lang="ts">
import Panel from 'primevue/panel';
import CategorisedLegendComponent from './LegendCategorisedComponent.vue';
import GraduatedLegendComponent from './LegendGraduatedComponent.vue';

const props = defineProps<{
  legendState: { state?: string; message?: string } | null;
  legendType: String | null;
  legendObject: Record<string, string>;
  fillOption: String | null;
}>();
</script>

<template>
  <div v-if="props.legendState" id="legend">
    <Panel class="flex-none">
      <template #header>
        <div class="flex items-center gap-2" style="margin-bottom: -10px;">
          <i class="pi pi-list" style="font-size: 1rem"></i>
          <span class="font-bold">Legend</span>
        </div>
      </template>
      <div v-if="props.legendState?.state !== 'valid'" class="flex flex-col items-center justify-center gap-2">
        <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
        <div class="opacity-50">{{ props.legendState?.message }}</div>
      </div>
      <template v-else>
        <CategorisedLegendComponent v-if="props.legendType === 'categorised'" :legendObject="props.legendObject"
          :fillOption="props.fillOption" />
        <GraduatedLegendComponent v-else-if="props.legendType === 'graduated'" :legendObject="props.legendObject"
          :fillOption="props.fillOption" />
      </template>
    </Panel>
  </div>
</template>

<style scoped>
#legend {
  position: absolute;
  bottom: 20px;
  right: 50px;
  width: 400px;
}
</style>