<script setup lang="ts">
import type { PropType } from 'vue';

defineProps({
  legendObject: {
    type: Object as PropType<Record<string, string>>,
    required: true,
  },
  fillOption: {
    type: String as PropType<String | null>,
    default: null,
  },
});
</script>

<template>
  <span class="subheading">{{ fillOption }}</span>
  <div v-for="(color, label) in legendObject" :key="label" class="legend-item">
    <span class="color-box" :style="{ backgroundColor: color }"></span>
    <span class="label">{{ label }}</span>
  </div>
</template>

<style scoped>
.legend-item {
  display: flex;
  align-items: center;
  margin-bottom: 4px;
}

.color-box {
  width: 15px;
  height: 15px;
  border-radius: 4px;
  margin-right: 8px;
  opacity: 0.8;
}

.subheading {
  font-size: 14px;
}

.label {
  font-size: 12px;
}
</style>