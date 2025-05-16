<script setup lang="ts">

const props = defineProps<{
  legendObject: Record<string, string>;
  fillOption: String | null;
}>();

</script>

<template>
  <span class="subheading">{{ props.fillOption }}</span>
  <div v-for="(color, label) in props.legendObject" :key="label" class="legend-item">
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
  flex-shrink: 0;
}

.subheading {
  font-size: 14px;
}

.label {
  font-size: 12px;
}
</style>