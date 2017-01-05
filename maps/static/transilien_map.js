// Tip: mapname and map_params defined in map.html template

//Create Base Layer, loads tiles from mapbox
var baseMap = new L.TileLayer('https://api.mapbox.com/styles/v1/leonardbinet/ciw0kj8c500b82klkfevbaje3/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1IjoibGVvbmFyZGJpbmV0IiwiYSI6ImNpdzBrNjU4NzAwMmwyb3BrYjQxemRoNnMifQ.7yzHGWbiQtCabkcgHa4oWw'
);

// Create the map
var map = new L.map('mapid', {
    center: new L.LatLng(48.866667, 2.333333),
    zoom: 8,
    maxZoom: 18,
    layers: baseMap
});
map.doubleClickZoom.disable();

// Point style
var stationStyle = {
  opacity: 0.9,
  fillOpacity: 0.7
};
// Line style
function setStyle(feature) {
    switch (feature.properties.severity.name) {
        case 'trip canceled': return {color: "#ff0000"};
        //case 'trip delayed':   return {color: "#0000ff"};
    }
}

// Create the control and add it to the map; layers will be added after
var control = L.control.layers();
control.addTo(map);

// We create each point with its style (from GeoJSON file)
function onEachFeature(feature, layer) {
    layer.bindPopup(function (layer) {
        return layer.feature.properties["Nom Gare"];
    });
}
// How JSON points will look
function pointToLayer(feature, latlng) {
    return L.circleMarker(latlng, stationStyle);
}

// We download the GeoJSON file
// Do this in the same scope as the actualiseGeoJSON function,
// so it can read the variable
$.getJSON(stations_json_url, // ajax view url
    {},
    initialLoad);

$.getJSON(ajaxdisruptionsurl, // ajax view url
    {
        map: mapname
    },
    initialLoadDisruptions);

function initialLoad(data){
    map.stopPointsLayer = L.geoJson(data,
        {onEachFeature: onEachFeature,
        pointToLayer: pointToLayer}
        )
    ;
    // CHECK
    map.markers = L.markerClusterGroup({
    //spiderfyOnMaxZoom: false,
    //showCoverageOnHover: false,
    //zoomToBoundsOnClick: false
});
    map.markers.addLayer(map.stopPointsLayer);
    map.addLayer(map.markers);
    // Add overlay to control panel
    control.addOverlay(map.markers, "Gares");
}
