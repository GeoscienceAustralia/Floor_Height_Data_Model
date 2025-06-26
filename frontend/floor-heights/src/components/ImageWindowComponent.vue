<script setup lang="ts">
import axios from 'axios';
import { ref, watch } from 'vue';
import Panel from 'primevue/panel';
import Tabs from 'primevue/tabs';
import TabList from 'primevue/tablist';
import Tab from 'primevue/tab';
import TabPanels from 'primevue/tabpanels';
import TabPanel from 'primevue/tabpanel';
import Button from 'primevue/button';
import Galleria from 'primevue/galleria';
import { Building } from "./types.ts";
import { useToast } from "primevue/usetoast";

const toast = useToast();

const isExpanded = ref(false);
const isClosed = ref(false);
const panoImages = ref<{ itemImageSrc: string }[]>([]);
const lidarImages = ref<{ itemImageSrc: string }[]>([]);

const props = defineProps<{
  building: Building | null;
}>();

const emit = defineEmits<{
  closeImageWindow: [windowClosed: boolean];
}>();

const toggleExpandWindow = () => {
  isExpanded.value = !isExpanded.value;
};

const closeWindow = () => {
  isClosed.value = false;
  emit('closeImageWindow', isClosed.value);
};

const fetchImages = async (buildingId: string, type: string) => {
  try {
    // Fetch the list of image IDs
    const idsResponse = await axios.get(`api/image-ids/${buildingId}?type=${type}`);
    const imageIds: string[] = idsResponse.data;

    // Fetch each image and store its blob URL
    const imagePromises = imageIds.map(async (imageId) => {
      const response = await axios.get(`api/image/${imageId}`, {
        responseType: 'arraybuffer',
      });
      const blob = new Blob([response.data], { type: 'image/jpeg' });
      return { itemImageSrc: URL.createObjectURL(blob) };
    });

    const images = await Promise.all(imagePromises);

    if (type === "panorama") {
      panoImages.value = images;
    } else if (type === "lidar") {
      lidarImages.value = images;
    }
  } catch (error) {
    console.error(`Failed to fetch images for building ID ${buildingId}`, error);
    toast.add({
      severity: 'error',
      summary: 'Error',
      detail: 'Failed to fetch floor measure images',
      life: 3000,
    });
  }
};

// Fetch the images when the component is mounted or when the building prop changes
watch(() => props.building?.id, (newId) => {
  if (newId) {
    fetchImages(newId, "panorama");
    fetchImages(newId, "lidar");
  }
}, { immediate: true });
</script>

<template>
  <div v-if="!isClosed" id="image-window" class="flex-col" :class="{ expanded: isExpanded }">
    <Panel class="flex-none">
      <div style="margin-top: -16px;">
        <Tabs value="0">
          <TabList>
            <div>
              <i class="pi pi-image" style="font-size: 1rem"></i>
            </div>
            <Tab value="0" class="tabheading">Pano</Tab>
            <Tab value="1" class="tabheading">LIDAR</Tab>
            <div class="flex" style="margin-left: auto; column-gap: 3px;">
              <Button @click="toggleExpandWindow" :icon="isExpanded ? 'pi pi-arrow-down-right' : 'pi pi-arrow-up-left'"
                class="button" />
              <Button @click="closeWindow" icon="pi pi-times" class="button" />
            </div>
          </TabList>
          <TabPanels style="padding: 0px;">
            <TabPanel value="0">
              <div v-if="panoImages.length > 0 && props.building != null">
                <Galleria :value="panoImages" :showItemNavigators="true" :showThumbnails="false" style="max-width: 640px">
                  <template #item="{ item }">
                    <img id="image" :src="item.itemImageSrc" alt="Panorama Image" :class="{ expanded: isExpanded }" />
                  </template>
                </Galleria>
              </div>
              <div id="image-placeholder" v-else-if="props.building == null" class="flex flex-col gap-2"
                :class="{ expanded: isExpanded }">
                <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
                <div class="opacity-50">Select a building to show panorama images.</div>
              </div>
              <div id="image-placeholder" v-else class="flex flex-col gap-2" :class="{ expanded: isExpanded }">
                <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
                <div class="opacity-50">No panorama images found for this building.</div>
              </div>
            </TabPanel>
            <TabPanel value="1">
              <div v-if="lidarImages.length > 0 && props.building != null">
                <Galleria :value="lidarImages" :showItemNavigators="true" :showThumbnails="false" style="max-width: 640px">
                  <template #item="{ item }">
                    <img id="image" :src="item.itemImageSrc" alt="Panorama Image" :class="{ expanded: isExpanded }" />
                  </template>
                </Galleria>
              </div>
              <div id="image-placeholder" v-else-if="props.building == null" class="flex flex-col gap-2"
                :class="{ expanded: isExpanded }">
                <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
                <div class="opacity-50">Select a building to show LIDAR images.</div>
              </div>
              <div id="image-placeholder" v-else class="flex flex-col gap-2" :class="{ expanded: isExpanded }">
                <i class="pi pi-info-circle opacity-25" style="font-size: 2rem"></i>
                <div class="opacity-50">No LIDAR images found for this building.</div>
              </div>
            </TabPanel>
          </TabPanels>
        </Tabs>
      </div>
    </Panel>
  </div>
</template>

<style scoped>
:deep(.p-galleria) {
  border-width: 0px;
}

:deep(.p-galleria .p-disabled) {
  color: transparent;
}

:deep(.p-galleria-next-button) {
  color: var(--p-primary-color);
}

:deep(.p-galleria-prev-button) {
  color: var(--p-primary-color);
}

:deep(.p-galleria-nav-button:not(.p-disabled):hover) {
  color: var(--p-button-primary-hover-background);
}

:deep(.p-tab-active) {
  color: var(--text-color);
}

#image-window {
  position: absolute;
  bottom: 20px;
  right: 50px;
  width: 400px;
  z-index: 1;
}

#image-window.expanded {
  width: 900px;
}

#image {
  width: 100%;
  height: 250px;
  object-fit: contain;
}

#image.expanded {
  height: 600px;
}

#image-placeholder {
  height: 250px;
  justify-content: center;
  align-items: center;
}

#image-placeholder.expanded {
  height: 600px;
}

.subheading {
  font-size: 14px;
}

.label {
  font-size: 12px;
}

.tabheading {
  /* font-size: 14px; */
  padding-top: 0px;
  padding-bottom: 10px;
}

.button {
  padding: 5px;
  width: 25px;
  height: 25px;
}
</style>