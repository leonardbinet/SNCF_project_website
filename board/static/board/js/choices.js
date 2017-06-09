(function(global){
    
    
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
    
    function selectLine(line){
        console.log("Line "+line+" is selected.")
        global.selectedLine = line;
        refreshTripChoice();
    }
    
    function refreshTripChoice(){
        $("#chosen-line-text").text(function(){
            if (!global.selectedLine){return "First choose your line."}
            return global.selectedLine;
        })
    }
    
    global.initDatatable = function(){
        global.datatable = $('#active-trains-table').DataTable( {
            columns: [
                { title: "Trip id", data: "Trip.trip_id"},
                { title: "Route id" , data: "Trip.route_id"},
                { title: "Service id", data: "Trip.service_id"},
                { title: "Direction", data: "Trip.direction_id"},
                { title: "Headsign", data: "Trip.trip_headsign"}
            ]
        } );
    };
    
    global.updateTableData = function(data){
        global.datatable.clear();
        global.datatable.rows.add(data);
        global.datatable.draw();
        
    };
    
    var lines = ['A', 'Aéroport C', 'B', 'C', 'D', 'E', 'H', 'J', 'K', 'L', 'N', 'P',
       'R', 'T4', 'U']
    // First init dropdown button:
    
    initDropDown(lines);
    refreshTripChoice();
    global.initDatatable();
    
}(window))