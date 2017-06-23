(function(global){

    // Ajax update Trips on selected Line
    global.ajaxCallTrips = function(selectedLine){
        console.log("Ajax call for datatable.")
        var url = "/api/trips/";
        var data = {
            level: 3,
            limit:500,
            on_route_short_name: selectedLine
        };
        var success = global.updateTableData.bind(this,global.tripDatatable);

        $.get(url, data, success)
    }

    // Ajax update StopTimes on selected Trip
    global.ajaxCallStopTimes = function(selectedTrip){
        console.log("Ajax call for datatable focused trip stoptimes.")
        var url = "/api/stoptimes/";

        var data = {
            realtime: true,
            trip_id_filter: selectedTrip,
            limit:50, // max number of elements (for pagination)
            level: 3, // to get StopTime, Stop
            active_at_time: false,
            on_day: false
        };

        var success = global.updateTableData.bind(this, global.focusedTripDatatable);

        $.get(url, data, success)
    }

    // Ajax update StopTimes Predictions on selected Trip
    global.ajaxCallPredictionStopTimes = function (selectedTrip){
        console.log("Ajax call for datatable focused trip stoptimes.")
        var url = "/api/trip-prediction/";

        var data = {
            trip_id: selectedTrip,
            limit: 50
        };
        var success = global.updateTableData.bind(this,global.focusedTripPredictionDatatable);

        $.get(url, data, success)
    }

}(window))