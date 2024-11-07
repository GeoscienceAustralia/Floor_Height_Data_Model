// Type definitions for client

interface AddressPoint {
  id: string;
  gnaf_id: string;
  address: string;
}

interface Building {
  id: string;
  height_ahd: number;
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
