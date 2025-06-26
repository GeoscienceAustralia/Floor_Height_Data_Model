// Type definitions for client

import { LngLat } from "maplibre-gl";

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
  confidence: number | null;
  aux_info?: Record<string, any> | null;
  method: string;
  datasets: string[];
}

interface MapLocation {
  label: string;
  coordinates: LngLat;
}

interface GraduatedFillLegend {
  min: number | null;
  max: number | null;
}

export type {
  AddressPoint,
  Building,
  FloorMeasure,
  MapLocation,
  GraduatedFillLegend
};
