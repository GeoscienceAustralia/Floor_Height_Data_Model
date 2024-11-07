// Type definitions for client

interface AddressPoint {
  id: string;
  gnaf_id: string;
  address: string;
}

interface Building {
  id: string;
  min_height_ahd: number;
  max_height_ahd: number;
}

interface FloorMeasure {
  id: string;
  storey: number;
  height: number;
  accuracy_measure: number;
  aux_info?: Record<string, any> | null;
  method: string;
  datasets: string[];
}

export type {
  AddressPoint,
  Building,
  FloorMeasure
};
