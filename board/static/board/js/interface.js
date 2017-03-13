
$(document).ready(function() {
    var table = $('#example').DataTable( {
        "ajax": {
            "url":"/api/trip/?trip_id=DUASN156466F01002-1_402666&info=schedule",
            "dataSrc": "",
        },
        "columns": [
            { 'data': "station_id" },
            { 'data': "scheduled_departure_time" },
            { 'data': "trip_id" },
            { 'data': "stop_sequence" },
            { 'data': "trip_headsign" },
            ]
        } );
    }
);
