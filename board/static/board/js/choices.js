(function(global){

    // Horizontal buttons
    function initDropDown(lines){
        for (var i=0; i<lines.length; i++){
            // for each line create button
            d3.select("#line-choice").selectAll(".line-buttons").data(lines).enter()
                .append("button")
                    .attr("type", "button")
                    .classed("btn btn-default line-buttons", true)
                    .text(function(d){return d;})
                    .on("click", selectLine)
        }
    }
    
    // Line selection on button
    function selectLine(line){
        console.log("Line "+line+" is selected.")
        global.selectedLine = line;
        refreshLineChoiceText();
        // empty table
        global.updateTableData(global.tripDatatable);
        // gets data in table
        ajaxCallTrips(line);
    }

    // Update selected Line
    function refreshLineChoiceText(){
        $("#chosen-line-text").text(function(){
            if (!global.selectedLine){return "First choose your line."}
            return global.selectedLine;
        })
    }

    // Ajax update Trips on selected Line
    function ajaxCallTrips(selectedLine){
        console.log("Ajax call for datatable.")
        var url = "/api/trips/";
        var data = {
            level: 3,
            on_route_short_name: selectedLine
        };
        var success = global.updateTableData.bind(this,global.tripDatatable);
        
        $.get(url, data, success)
    }
    
    // Ajax update StopTimes on selected Trip
    function ajaxCallStopTimes(selectedTrip){
        console.log("Ajax call for datatable focused trip stoptimes.")
        var url = "/api/stoptimes/";
        var data = {
            realtime: true,
            trip_id_filter: selectedTrip,
            level: 3 // to get StopTime, Stop
        };
        var success = global.updateTableData.bind(this,global.focusedTripDatatable);
        
        $.get(url, data, success)
    }
    
    // On click on table
    function tripTableInteractionInit(){
        $('#active-trains-table tbody').on('click', 'tr', function () {
            var data = global.tripDatatable.row( this ).data();
            onClickTripRow(data);
            
            // Update class: selected for row display
            if ( $(this).hasClass('selected') ) {
                $(this).removeClass('selected');
            }
            else {
                global.tripDatatable.$('tr.selected').removeClass('selected');
                $(this).addClass('selected');
            }
        } );
    }
    
    
    function onClickTripRow(data){
        // set focused trip
        global.focusedTripData = data;
        // refresh what is written in focus text
        $("#chosen-trip-text").text(function(){
            if (!data){return "First choose your trip (click on row)."}
            return "you chose "+data.Trip.trip_id;
        })
        // emptu table
        global.updateTableData(global.focusedTripDatatable);
        // send ajax call to update stoptimes of trip
        ajaxCallStopTimes(data.Trip.trip_id);
        
    }
    // INIT
    // Lines init
    var lines = ['A', 'Aéroport C', 'B', 'C', 'D', 'E', 'H', 'J', 'K', 'L', 'N', 'P',
       'R', 'T4', 'U']
    
    // First init dropdown button:
    initDropDown(lines);
    // Text for line
    refreshLineChoiceText();
    // Datatables creation
    global.initDatatables();
    tripTableInteractionInit();
    
    

}(window))
