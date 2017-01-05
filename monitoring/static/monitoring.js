// First check MongoDB connection


function updateHtml(response){
    var status = response['status'];
    var add_info = response['add_info'];
    var printed_info = "Databases available: "+JSON.stringify(add_info['database_names'])
    var buttonClass;
    var faviconClass;
    if (status==true) {
        buttonClass="btn btn-success btn-circle btn-lg";
        faviconClass="fa fa-check";}
    else if (status==false) {
        buttonClass="btn btn-danger btn-circle btn-lg";
        faviconClass="fa fa-times";}

    else {
        buttonClass="btn";
    }
    $("#mongo-status-circle").attr('class', buttonClass);
    $("#mongo-status-fa").attr('class', faviconClass);
    $("#mongo-add-info").html(printed_info);

    // Now create divs and display bars
    var databasesStats = response['add_info']['databases_stats']
    var arrayLength = databasesStats.length;
    for (var i = 0; i < arrayLength; i++) {
        var databaseName = databasesStats[i]["database"];
        var databaseData = databasesStats[i]["collections"];
        createDiv(databaseName);
        showMongoCollStats(databaseName,databaseData);
    }
}

function checkMongoConnection(callback) {
    $.getJSON(mongo_ajax_url,
        {
            connection_id : "connection_id"
        },
        updateHtml);

    callback();

}

function createDiv(databaseName){
    var newDiv = "<h3> Database: "+databaseName+"</h3><div id='"+databaseName+"' style='height: 250px;'></div>";
    $("#databases_stats").append(newDiv)
}

function showMongoCollStats(databaseName,databaseData){
Morris.Bar({
  element: databaseName,
  data: databaseData,
  xkey: 'collection',
  ykeys: ['count'],
  labels: ['Count']
});
}

/*
Morris.Bar({
  element: databaseName,
  data: [
    { col: 'stat', count: 75},
    { col: 'disruptions', count: 40},
    { col: 'new', count: 24},
    { col: 'other', count: 75},
  ],
  xkey: 'col',
  ykeys: ['count'],
  labels: ['Count']
});
*/
