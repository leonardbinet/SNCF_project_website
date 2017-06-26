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
            emptyTables();
            return global.selectedLine;
        })
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

        emptyTables()

        // send ajax call to update stoptimes of trip
        global.ajaxCallStopTimes(data.Trip.trip_id);
        global.ajaxCallPredictionStopTimes(data.Trip.trip_id);
    }

    function emptyTables(){
        // empty tables
        //global.updateTableData(global.focusedTripDatatable);
        global.updateTableData(global.focusedTripPredictionDatatable);
    }

    // INIT
    // Lines init
    var lines = ['C', 'D', 'E', 'H', 'J', 'K', 'N', 'P', 'U']
    
    // First init dropdown button:
    initDropDown(lines);
    // Text for line
    refreshLineChoiceText();
    // Datatables creation
    global.initDatatables();
    // Interaction
    tripTableInteractionInit();

}(window))
