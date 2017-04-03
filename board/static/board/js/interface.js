
$(document).ready(function() {
    var table = $('#example').DataTable( {
        "pageLength": 20,
        "ajax": {
            "url":ajaxUrl,
            "dataSrc": "",
        },
        "columns": [
            { 'data': "station_id" },
            { 'data': "scheduled_departure_time" },
            { 'data': "trip_id" },
            { 'data': "stop_sequence" },
            { 'data': "trip_headsign" },
            { 'data': "day_train_num" },
            { 'data': "expected_passage_time" },
            ]
        } );
    }
);
