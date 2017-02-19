function checkMongoConnection(callback) {
    $.getJSON(mongo_ajax_url,
        updateHtmlMongo);
}

function checkDynamoConnection(callback) {
    $.getJSON(dynamo_ajax_url,
        updateHtmlDynamo);
}

function updateHtmlMongo(response){
    var db = "mongo";
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
    $("#"+db+"-status-circle").attr('class', buttonClass);
    $("#"+db+"-status-fa").attr('class', faviconClass);
    $("#"+db+"-add-info").html(printed_info);

    // Now create divs and display bars

    var databasesStats = response['add_info']['databases_stats'];
    var arrayLength = databasesStats.length;
    // clear div if needed
    $("#"+db+"_div").empty();
    for (var i = 0; i < arrayLength; i++) {
        var databaseName = databasesStats[i]["database"];
        var databaseData = databasesStats[i]["collections"];
        createDiv(databaseName, "#"+db+"_div");
        showStats(databaseName,databaseData,"collections");
    }
    $('#mongo-refresh').button('reset');

}

function updateHtmlDynamo(response){
    var db = "dynamo";
    var status = response['status'];
    var add_info = response['add_info'];
    var printed_info = JSON.stringify(add_info['tables_desc']);

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
    $("#"+db+"-status-circle").attr('class', buttonClass);
    $("#"+db+"-status-fa").attr('class', faviconClass);
    $("#"+db+"-add-info").html(printed_info);

    // Now create divs and display bars
    var databaseData = response['add_info']['tables_stats'];
    var newDiv = "<h3> Database: dynamo</h3><div id='dynamo' style='height: 250px;'></div>";
    $("#dynamo_div").append(newDiv)
    showStats("dynamo",databaseData, "table");

    $('#dynamo-refresh').button('reset');

}



function createDiv(databaseName, dbId){
    var newDiv = "<h3> Database: "+databaseName+"</h3><div id='"+databaseName+"' style='height: 250px;'></div>";
    $(dbId).append(newDiv)
}

function showStats(databaseName,databaseData, xkey){
    Morris.Bar({
      element: databaseName,
      data: databaseData,
      xkey: xkey,
      ykeys: ['count'],
      labels: ['Count']
    });
}


$('#mongo-refresh').on('click', function () {
    var btn = $(this).button('loading');
    checkMongoConnection();
    });

$('#dynamo-refresh').on('click', function () {
    var btn = $(this).button('loading');
    checkDynamoConnection();
    });
