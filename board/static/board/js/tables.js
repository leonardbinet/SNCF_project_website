(function(global){
    
    global.initDatatables = function(){
        
        // Trip table
        global.tripDatatable = $('#active-trains-table').DataTable( {
                processing:true,
                language: {
                    processing: "Loading..."
                },
            columns: [
                { title: "Trip id", data: "Trip.trip_id"},
                { title: "Service id", data: "Trip.service_id"},
                { title: "Direction", data: "Trip.direction_id"},
                { title: "Headsign", data: "Trip.trip_headsign"},
                { title: "Route", data: "Route.route_long_name", "defaultContent": "not found"},
            ]
        } );
        
        
        // Trip focus Stoptimes table
        global.focusedTripDatatable = $('#focused-trip-stoptimes-table').DataTable( {
            "processing":true,
            "language": {
                    "processing": "Loading..."
                },
            columns: [
                { title: "Stop sequence", data: "StopTime.stop_sequence"},
                { title: "Scheduled departure time" , data: "StopTime.departure_time"},
                { title: "Stop name", data: "Stop.stop_name"},
                { title: "API departure time", data: "RealTime.expected_passage_time", "defaultContent": "not found"},
                { title: "Data freshness", data: "RealTime.data_freshness", "defaultContent": "not found"}

            ]
        } );

        global.focusedTripPredictionDatatable = $('#focused-trip-prediction-stoptimes-table').DataTable( {
            "processing":true,
            "language": {
                    "processing": "Loading..."
                },
            columns: [
                { title: "Stop sequence", data: "StopTime.stop_sequence"},
                { title: "Scheduled departure time" , data: "StopTime.departure_time"},
                { title: "Stop name", data: "Stop.stop_name"},
                { title: "Delay observed", data: "StopTimeState.delay", "defaultContent": "not found"},
                { title: "Data freshness", data: "RealTime.data_freshness", "defaultContent": "not found"},
                { title: "Passed (schedule)", data: "StopTimeState.passed_schedule", "defaultContent": "not found"},
                { title: "Passed (realtime)", data: "StopTimeState.passed_realtime", "defaultContent": "not found"},
                { title: "To predict?", data: "to_predict", "defaultContent": "not found"},
                { title: "Delay prediction", data: "prediction", "defaultContent": "not found"},
            ]
        } );
    };

    global.updateTableData = function(table, data){
        // if no data provided: empty datatable
        var results = [];
        if (data){results = data.results; global.data = data;}
        table.clear();
        table.rows.add(results);
        table.draw();

    };
}(window))