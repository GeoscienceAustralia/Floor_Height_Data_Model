import { Map, Popup, LngLatBoundsLike, MapGeoJSONFeature, LngLatLike, } from 'maplibre-gl';

import { Point} from 'geojson';

import 'maplibre-gl/dist/maplibre-gl.css';

import { EventEmitter } from '../util/EventEmitter';

const TILESERVER_LAYER_DETAILS = [
  {name: 'address_point', idField: 'id'},
  {name: 'building', idField: 'id'},
];

export default class FloorHeightsMap {
  map: Map | null;
  emitter: EventEmitter;

  constructor() {
    this.map = null;
    this.emitter = new EventEmitter();
  }

  createMap() {
    return new Promise((resolve) => {
      this.map = new Map({
        container: 'map',
        style: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
        // style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
        // // NSW bounds
        // bounds: [[140.7002, -37.4088], [153.9388, -27.8566]],
        // Wagga Wagga bounds
        bounds: [[147.255, -35.052], [147.485, -35.252]],
      });
      
      this.map.on('add', async () => {
      });
      
      this.map.on('load', async () => {
        // add all the layers as sources, this doesn't mean they get displayed
        for (const layerDetails of TILESERVER_LAYER_DETAILS) {
          const layerName = layerDetails.name;
          this.map?.addSource(layerName, {
              type: 'vector',
              tiles: [
                  `${window.location.href}maps/${layerName}/{z}/{x}/{y}`
              ],
              minzoom: 0,
              maxzoom: 22,
              promoteId: layerDetails.idField
          });
        }

        return resolve(this.map);
      });
      
      this.map.on('click', 'address_point', (e) => {
        let f = e.features?.[0];

        // // The issue with the following code is that the `flyTo`
        // // seems to make the map lose focus and click events
        // // are no longer raised.
        // let p = f?.geometry as Point;
        // this.map?.flyTo({
        //     center: p.coordinates as LngLatLike,
        //     zoom: 19
        // });

        // show the links to any buildings when an address point is clicked
        this.showBuildingLinksForAddress(f?.properties.id);

        this.emitter.emit('addressPointClicked', f?.properties);
      });

      this.map.on('click', 'building_fh', (e) => {
        let f = e.features?.[0];
        this.emitter.emit('buildingClicked', f?.properties);
      });
    })
  }

  fitBounds (bounds: LngLatBoundsLike) {
    this.map?.fitBounds(bounds, {
      padding: 60
    });
  }
  
  setBuildingOutlineVisibility(visible: boolean) {
    if (visible) {
      this.map?.addLayer({
        'id': 'building_fh',
        'type': 'fill',
        'source': 'building',
        'source-layer': 'building',
        'layout': {},
        'paint': {
          'fill-color': '#F6511D',
          'fill-outline-color': '#F6511D',
          'fill-opacity': 0.4,
        }
      }, 'address_point');
    } else {
      if (this.map?.getLayer('building_fh')) {
        this.map?.removeLayer('building_fh');
      }
    }
  }
  
  setAddressPointVisibility(visible: boolean) {
    if (visible) {
      this.map?.addLayer({
        'id': 'address_point',
        'type': 'circle',
        'source': 'address_point',
        'source-layer': 'address_point',
        'paint': {
          'circle-color': '#3887BE',
          'circle-radius': 5,
        }
      });
    } else {
      if (this.map?.getLayer('address_point')) {
        this.map?.removeLayer('address_point');
      }
      this.hideBuildingLinksForAddress();
    }
  }

  hideBuildingLinksForAddress() {
    if (this.map?.getLayer('address-to-building-link')) {
      this.map?.removeLayer('address-to-building-link');
    }
    if (this.map?.getSource('address-to-building-link')) {
      this.map?.removeSource('address-to-building-link');
    }
  }

  showBuildingLinksForAddress(addressPointId: string) {
    // remove the source and layer if it has already been added
    this.hideBuildingLinksForAddress();

    this.map?.addSource('address-to-building-link', {
      type: 'geojson',
      data: `api/address-point-to-building/${addressPointId}/geom/`
    });
    this.map?.addLayer({
      'id': 'address-to-building-link',
      'type': 'line',
      'source': 'address-to-building-link',
      'layout': {},
      'paint': {
        'line-color': '#FFB400',
        'line-width': 4
      },
    }, 'address_point');
  }

}
