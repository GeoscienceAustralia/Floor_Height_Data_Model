<script setup lang="ts">

const props = defineProps<{
  legendObject: Record<string, string>;
  fillOption: String | null;
}>();

</script>

<template>
  <span class="subheading">{{ fillOption }} (m)</span>
  <div class="legend-item">
    <div
      class="gradient-box"
      :style="{
        background: `linear-gradient(to right, ${Object.values(props.legendObject).join(', ')})`,
      }"
    ></div>
    <div class="gradient-label">
      <span class="label">{{ Number(Object.keys(props.legendObject)[0]).toFixed(1) }}</span>
      <span class="label">{{ Number(Object.keys(props.legendObject)[1]).toFixed(1) }}</span>
      <span class="label">{{ Number(Object.keys(props.legendObject)[2]).toFixed(1) }}</span>
    </div>
  </div>
</template>

<style scoped>
.legend-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-top: 3px;
}

.gradient-box {
  width: 100%;
  height: 15px;
  border-radius: 4px;
  margin-bottom: 4px;
  opacity: 0.8;
}

.gradient-label {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.subheading {
  font-size: 14px;
}

.label {
  font-size: 12px;
}
</style>
