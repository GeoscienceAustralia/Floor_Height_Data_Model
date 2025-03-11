import { Map, LngLatBoundsLike, AddLayerObject, NavigationControl } from 'maplibre-gl';

import { Point } from 'geojson';

import 'maplibre-gl/dist/maplibre-gl.css';

import { EventEmitter } from '../util/EventEmitter';

const TILESERVER_LAYER_DETAILS = [
  {name: 'address_point', idField: 'id'},
  {name: 'building', idField: 'id'},
  {name: 'building_query', idField: 'id'}
];

const COLOR_ADDRESS_POINT: string = '#3887BE';
const COLOR_BUILDING: string = '#F6511D';
const COLOR_ADDRESS_BUILDING_LINK: string = '#3887BE';

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
        center: [147.360, -35.120],
        zoom: 12,
        maxZoom: 22,
        minZoom: 3
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

      this.map.addControl(
        new NavigationControl({
          visualizePitch: true,
          showZoom: true,
          showCompass: true,
        }), 'bottom-right'
      );

      this.map.on('click', 'address_point', (e) => {
        let f = e.features?.[0];
        this.highlightAddressPoint(f?.geometry as Point);
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
        console.log(f);
        this.highlightBuilding(f?.properties.id);
        this.emitter.emit('buildingClicked', f?.properties);
      });
    })
  }

  setCenter(center: [number, number]): void {
    this.map?.setCenter(center);
  }

  setZoom(zoom: number): void {
    this.map?.setZoom(zoom);
  }

  fitBounds (bounds: LngLatBoundsLike) {
    this.map?.fitBounds(bounds, {
      padding: 60
    });
  }

  generateCombinations = (array: string[]) => {
    const results: string[][] = [];
    const totalCombinations = 1 << array.length;

    for (let i = 1; i < totalCombinations; i++) {
      const combination: string[] = [];
      for (let j = 0; j < array.length; j++) {
        if (i & (1 << j)) {
          combination.push(array[j]);
        }
      }
      results.push(combination);
    }
    return results;
  }

  generateColor = (str: string) => {
    let hash = 0;
    // Use a hash function for consistent colour generation
    for (let i = 0; i < str.length; i++) {
      hash = str.charCodeAt(i) + ((hash << 5) - hash);
      hash = hash & hash;
    }
    const hue = Math.abs(hash % 360);
    return `hsl(${hue}, 70%, 50%)`;
  }

  setBuildingMethodCategorisedFill(methods: string[]) { 
    methods.sort()
    // Generate Cartesian product of the methods
    const combinations = this.generateCombinations(methods);

    // Assign unique colour to each combination
    const combinationColors = combinations.reduce((acc, combination) => {
      const key = combination.join(', ');
      acc[key] = this.generateColor(key);
      return acc;
    }, {} as Record<string, string>);

    // Construct the match expression
    const matchExpression = ['match', ['get', 'method_names']];

    for (const [key, color] of Object.entries(combinationColors)) {
      matchExpression.push(key, color); // Add combination and its colour
    }
    matchExpression.push('#FFFFFF'); // Assign a colour for empty categories

    if (this.map?.getSource('building_query')) {
      this.map?.getSource('building_query')?.setTiles([
          `${window.location.href}maps/building_query/{z}/{x}/{y}?${new URLSearchParams(methods.toString())}`
        ]);
    }

    if (this.map?.getLayer('building_fh')) {
      this.map?.setPaintProperty('building_fh', 'fill-color', matchExpression);
      this.map?.setPaintProperty('building_fh', 'fill-outline-color', matchExpression);
      this.map?.setPaintProperty('building_fh', 'fill-opacity', 0.4);
    }
  }

  // https://maplibre.org/maplibre-gl-js/docs/examples/color-switcher/
  setBuildingFloorHeightGraduatedFill(method_name: string) {
    let queryParams = {
      method_name: method_name,
      // other_param: 'other value'
    };

    if (this.map?.getSource('building_query')) {
      // this.map?.removeSource('building_query');
      this.map?.getSource('building_query')?.setTiles([
          `${window.location.href}maps/building_query/{z}/{x}/{y}?${new URLSearchParams(queryParams).toString()}`
        ]);
    }

    if (this.map?.getLayer('building_fh')) {
      this.map?.setPaintProperty('building_fh', 'fill-color', [
        'interpolate',
        ['linear'],
        ['get', 'avg_ffh'],
        0,
        '#FDD9CE',
        1,
        '#F98D6C',
        2,
        '#F7673B',
        3,
        '#F6511D',
        4,
        '#C43408',
        5,
        '#932706',
      ]);
      this.map?.setPaintProperty('building_fh', 'fill-outline-color', [
        'interpolate',
        ['linear'],
        ['get', 'avg_ffh'],
        0,
        '#FDD9CE',
        1,
        '#F98D6C',
        2,
        '#F7673B',
        3,
        '#F6511D',
        4,
        '#C43408',
        5,
        '#932706',
      ]);
      this.map?.setPaintProperty('building_fh', 'fill-opacity', 0.6);

    }
  }

  resetBuildingTiles(): void {
    if (this.map?.getSource('building_query')) {
      // this.map?.removeSource('building_query');
      this.map?.getSource('building_query')?.setTiles([
        `${window.location.href}maps/building_query/{z}/{x}/{y}`
      ]);
      this.map?.setPaintProperty('building_fh', 'fill-color', COLOR_BUILDING);
      this.map?.setPaintProperty('building_fh', 'fill-outline-color', COLOR_BUILDING);
      this.map?.setPaintProperty('building_fh', 'fill-opacity', 0.4);
    }
  }

  setBuildingOutlineVisibility(visible: boolean) {
    if (visible) {
      let buildingLayerDef:AddLayerObject = {
        'id': 'building_fh',
        'type': 'fill',
        'source': 'building_query',
        'source-layer': 'building_query',
        'layout': {},
        'paint': {
          'fill-color': COLOR_BUILDING,
          'fill-outline-color': COLOR_BUILDING,
          'fill-opacity': 0.4,
        }
      };
      if (this.map?.getLayer('address_point')) {
        this.map?.addLayer(buildingLayerDef, 'address_point');
      } else {
        this.map?.addLayer(buildingLayerDef);
      }
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
          'circle-color': COLOR_ADDRESS_POINT,
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
        'line-color': COLOR_ADDRESS_BUILDING_LINK,
        'line-width': 2,
        "line-dasharray": ["literal", [3, 1]]
      },
    }, 'address_point');
  }

  setMethodFilter(methods: string[]) {
    if (methods.length == 0) {
      this.map?.setFilter('building_fh', null);
      return;
    }
    const filterExpression = [
        "any",
        ...methods.map(name => ["!", ["==", ["index-of", name, ["get", "method_names"]], -1]])
    ];

    this.map?.setFilter('building_fh', filterExpression);
  }

  hideHighlightedFeature() {
    if (this.map?.getLayer('highlighted-feature')) {
      this.map?.removeLayer('highlighted-feature');
    }
    if (this.map?.getSource('highlighted-feature')) {
      this.map?.removeSource('highlighted-feature');
    }
  }

  highlightAddressPoint(geometry: Point) {
    this.hideHighlightedFeature();

    this.map?.addSource('highlighted-feature', {
      type: 'geojson',
      data: {
        type: 'Feature',
        geometry: geometry, // Use the geometry from the clicked feature
        properties: {}
      }
    });

    // Add a new layer to render the geometry
    this.map?.addLayer({
      id: 'highlighted-feature',
      type: 'circle',
      source: 'highlighted-feature',
      paint: {
        'circle-color': COLOR_ADDRESS_POINT,
        'circle-radius': 14,
        'circle-blur': 1.0
      }
    }, 'address_point');
  }

  highlightBuilding(buildingId: string) {
    this.hideHighlightedFeature();

    this.map?.addSource('highlighted-feature', {
      type: 'geojson',
      data: `api/building/${buildingId}/geom/`
    });

    // Add a new layer to render the geometry
    this.map?.addLayer({
      id: 'highlighted-feature',
      type: 'line',
      source: 'highlighted-feature',
      paint: {
        'line-color': COLOR_BUILDING,
        'line-width': 6,
        'line-blur': 4
      }
    }, 'building_fh');
  }
}
