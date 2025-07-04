import { Map, LngLat, LngLatBoundsLike, NavigationControl, VectorTileSource} from 'maplibre-gl';

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

// Matplotlib magma gradient - https://matplotlib.org/stable/users/explain/colors/colormaps.html#sequential
const COLOR_BUILDING_GRADIENT_CLASSES: string[] = ['#FED395', '#F1605D', '#331067'];

// Matplotlib tab20 palette - https://matplotlib.org/stable/users/explain/colors/colormaps.html#qualitative
const COLOR_BUILDING_CATEGORISED_CLASSES: string[] = [
  '#1F77B4', '#FF7F0E', '#2CA02C', '#D62728', '#9467BD',
  '#8C564B', '#E377C2', '#7F7F7F', '#BCBD22', '#17BECF',
  '#AEC7E8', '#FFBB78', '#98DF8A', '#FF9896', '#C5B0D5',
  '#C49C94', '#F7B6D2', '#C7C7C7', '#DBDB8D', '#9EDAE5'
];

export default class FloorHeightsMap {
  map: Map | null;
  emitter: EventEmitter;

  constructor() {
    this.map = null;
    this.emitter = new EventEmitter();
  }

  createMap(mapCenter: LngLat) {
    return new Promise((resolve) => {
      this.map = new Map({
        container: 'map',
        style: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
        // style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
        center: mapCenter,
        zoom: 13,
        maxZoom: 22,
        minZoom: 11
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
                  `${window.location.href}api/maps/${layerName}/{z}/{x}/{y}`
              ],
              minzoom: 0,
              maxzoom: 22,
              promoteId: layerDetails.idField
          });
        }
        
        // Add building layer
        this.map?.addLayer({
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
        });

        // Add address layer
        this.map?.addLayer({
          'id': 'address_point',
          'type': 'circle',
          'source': 'address_point',
          'source-layer': 'address_point',
          'paint': {
            'circle-color': COLOR_ADDRESS_POINT,
            'circle-radius': [
              'interpolate', ['linear'], ['zoom'],
              14, 1,  // when zoomed out, radius is 1
              18, 5  // when zoomed in, radius is 5
            ],
          }
        });

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

  generateCategorisedColorMap = (attributes: string[]) => {
    // Generate a unique colour for each attribute using the predefined colour array
    const attributeColors = attributes.reduce((acc, attribute, index) => {
      acc[attribute] = COLOR_BUILDING_CATEGORISED_CLASSES[index % COLOR_BUILDING_CATEGORISED_CLASSES.length];
      return acc;
    }, {} as Record<string, string>);
  
    // Construct the colour map
    const colorMap = [];
  
    for (const [attribute, color] of Object.entries(attributeColors)) {
      colorMap.push(attribute, color); // Add attribute and its assigned colour
    }
  
    colorMap.push('#FFFFFF'); // Assign a colour for empty categories
  
    return colorMap;
  }

  setBuildingCategorisedFill(methods: string[], datasets: string[], colorMap: string[], field: string, locationBounds: LngLatBoundsLike) {
    let queryParams: Record<string, string> = {
      method_filter: methods.toString(),
      dataset_filter: datasets.toString(),
      bbox: locationBounds.toString()
    };
  
    const matchExpression = ['match', ['get', field]].concat(colorMap);

    if (this.map?.getSource('building_query')) {
      (this.map?.getSource('building_query') as VectorTileSource)?.setTiles([
          `${window.location.href}api/maps/building_query/{z}/{x}/{y}?${new URLSearchParams(queryParams)}`
        ]);
    }

    if (this.map?.getLayer('building_fh')) {
      this.map?.setPaintProperty('building_fh', 'fill-color', matchExpression);
      this.map?.setPaintProperty('building_fh', 'fill-outline-color', matchExpression);
      this.map?.setPaintProperty('building_fh', 'fill-opacity', 0.4);
    }
  }

  generateGraduatedColorMap = (min: number, max: number) => {
    const colorMap = [
      min, COLOR_BUILDING_GRADIENT_CLASSES[0],
      Math.abs(min+max)/2, COLOR_BUILDING_GRADIENT_CLASSES[1],
      max, COLOR_BUILDING_GRADIENT_CLASSES[2]
    ];
    return colorMap;
  }

  setBuildingFloorHeightGraduatedFill(methods: string[], datasets: string[], colorMap: string[], locationBounds: LngLatBoundsLike) {
    let queryParams: Record<string, string> = {
      method_filter: methods.toString(),
      dataset_filter: datasets.toString(),
      bbox: locationBounds.toString()
    };

    const matchExpression = ['interpolate', ['linear'], ['get', 'avg_ffh']].concat(colorMap);
  
    if (this.map?.getSource('building_query')) {
      (this.map?.getSource('building_query') as VectorTileSource)?.setTiles([
          `${window.location.href}api/maps/building_query/{z}/{x}/{y}?${new URLSearchParams(queryParams)}`
        ]);
    }

    if (this.map?.getLayer('building_fh')) {
      this.map?.setPaintProperty('building_fh', 'fill-color', matchExpression);
      this.map?.setPaintProperty('building_fh', 'fill-outline-color', matchExpression);
      this.map?.setPaintProperty('building_fh', 'fill-opacity', 0.7);
    }
  }

  resetBuildingTiles(): void {
    if (this.map?.getSource('building_query')) {
      (this.map?.getSource('building_query') as VectorTileSource)?.setTiles([
        `${window.location.href}api/maps/building_query/{z}/{x}/{y}`
      ]);
      this.map?.setPaintProperty('building_fh', 'fill-color', COLOR_BUILDING);
      this.map?.setPaintProperty('building_fh', 'fill-outline-color', COLOR_BUILDING);
      this.map?.setPaintProperty('building_fh', 'fill-opacity', 0.4);
    }
  }

  setBuildingOutlineVisibility(visible: boolean) {
    if (!visible) {
      this.map?.setLayoutProperty("building_fh", "visibility", "none");
    } else {
      this.map?.setLayoutProperty("building_fh", "visibility", "visible");
    }
  }

  setAddressPointVisibility(visible: boolean) {
    if (!visible) {
      this.map?.setLayoutProperty("address_point", "visibility", "none");
      this.hideBuildingLinksForAddress();
    } else {
      this.map?.setLayoutProperty("address_point", "visibility", "visible");
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

  setBuildingFilter(datasets: string[], methods: string[]) {
    if (datasets.length == 0 && methods.length == 0) {
      this.map?.setFilter('building_fh', null);
      return;
    }
    const filterExpression = [
        "any",
        ...datasets.map(name => ["!", ["==", ["index-of", name, ["get", "dataset_names"]], -1]]),
        ...methods.map(name => ["!", ["==", ["index-of", name, ["get", "method_names"]], -1]])
    ];

    // @ts-ignore
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
