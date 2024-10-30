import { Map, Popup } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

const TILESERVER_LAYER_DETAILS = [
  {name: 'address_point', idField: 'id'},
  {name: 'building', idField: 'id'},
];

export default class FloorHeightsMap {
  constructor() {
    this.map = null
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
          this.map.addSource(layerName, {
              type: 'vector',
              tiles: [
                  `${window.location.href}maps/${layerName}/{z}/{x}/{y}`
              ],
              minzoom: 0,
              maxzoom: 22,
              promoteId: layerDetails.idField
          });
        }

        return resolve(this.map)
      });
      
    })
  }
  
  fitBounds (bounds) {
    this.map.fitBounds(bounds, {
      padding: 60
    })
  }
  
  setBuildingOutlineVisibility(visible) {
    if (visible) {
      this.map.addLayer({
        'id': 'building_fh',
        'type': 'line',
        'source': 'building',
        'source-layer': 'building',
        'layout': {
          'line-join': 'round',
          'line-cap': 'round'
        },
        'paint': {
          'line-color': '#ff69b4',
          'line-width': 1
        }
      });

    } else {
      if (this.map.getLayer('building_fh')) {
        this.map.removeLayer('building_fh');
      }
    }
  }
  
  setAddressPointVisibility(visible) {

  }
}
