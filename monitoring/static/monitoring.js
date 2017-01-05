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

}

function checkMongoConnection(callback) {
    $.getJSON(mongo_ajax_url,
        {
            connection_id : "connection_id"
        },
        updateHtml);

    callback();

}
