// Tip: mapname and map_params defined in map.html template

//Create Base Layer, loads tiles from mapbox
var baseMap = new L.TileLayer('https://api.mapbox.com/styles/v1/leonardbinet/ciw0kj8c500b82klkfevbaje3/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1IjoibGVvbmFyZGJpbmV0IiwiYSI6ImNpdzBrNjU4NzAwMmwyb3BrYjQxemRoNnMifQ.7yzHGWbiQtCabkcgHa4oWw'
);

// Create the map
var map = new L.map('mapid', {
    center: new L.LatLng(46.5, 2.5),
    zoom: 6,
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
        return layer.feature.properties[marker_label];
    });
}
// How JSON points will look
function pointToLayer(feature, latlng) {
    return L.circleMarker(latlng, stationStyle);
}

// We download the GeoJSON file
// Do this in the same scope as the actualiseGeoJSON function,
// so it can read the variable
$.getJSON(ajaxurl, // ajax view url
    {
        lat: 46.5,
        lng: 2.5,
        map: mapname
    },
    initialLoad);

$.getJSON(ajaxdisruptionsurl, // ajax view url
    {
        map: mapname
    },
    initialLoadDisruptions);

function initialLoad(data){
    map.stopPointsLayer = L.geoJson(data[mapcollection],
        {onEachFeature: onEachFeature,
        pointToLayer: pointToLayer}
        )
    ;
    // CHECK
    map.markers = L.markerClusterGroup({
    //spiderfyOnMaxZoom: false,
    showCoverageOnHover: false,
    //zoomToBoundsOnClick: false
});
    map.markers.addLayer(map.stopPointsLayer);
    map.addLayer(map.markers);
    // Add overlay to control panel
    control.addOverlay(map.markers, "Gares");
}


function onEachFeatureDisruption(feature, layer) {
    layer.bindPopup(function (layer) {
        return layer.feature.properties.label;
    });
}

function initialLoadDisruptions(data){
    map.delayLayer = L.geoJson(data["delayed"],{onEachFeature: onEachFeatureDisruption});
    map.canceledLayer = L.geoJson(data["canceled"],{onEachFeature: onEachFeatureDisruption, style:setStyle});

    map.addLayer(map.delayLayer);
    map.addLayer(map.canceledLayer);
    // Add overlay to control panel
    control.addOverlay(map.delayLayer, "Perturbations: retards : "+data["delayed"].length);
    control.addOverlay(map.canceledLayer, "Perturbations: annulations : "+data["canceled"].length);

}

function refreshGeoJsonLayer(latlng) {
    // Then get data, and add it back to the layer
    // TODO query MongoDB with parameter in bounds
    $.getJSON(ajaxurl, // ajax view url
        {
            lat: latlng.lat,
            lng: latlng.lng,
            map: mapname // so the ajax knows what to send
        },
        function (data) {
        // First we clear the layer
        map.stopPointsLayer.clearLayers();
        map.stopPointsLayer.addData(data[mapcollection]);
        map.markers.clearLayers();
        map.markers.addLayer(map.stopPointsLayer);
    });
}

function refreshDisruptions(){
    $.getJSON(ajaxdisruptionsurl, // ajax view url
        {
            map: mapname // so the ajax knows what to send
        },
        function (data) {
        // First we clear the layer

    });
}

function onMapDoubleClick(e) {
    var latlng = e.latlng;
    refreshGeoJsonLayer(latlng);
    refreshDisruptions();
}

// Datas are modified if
map.on('dblclick', onMapDoubleClick);
