function updateHtml(response){
    var status = response['status'];
    var buttonClass;
    var faviconClass;
    if (status==true) {
        buttonClass="btn btn-success btn-circle btn-lg";
        faviconClass="fa fa-check";}
    else {
        buttonClass="btn btn-danger btn-circle btn-lg";
        faviconClass="fa fa-times";}

    $("#update-status-circle").attr('class', buttonClass);
    $("#update-status-fa").attr('class', faviconClass);


}

function updateDisruptions(callback) {
    $.getJSON(update_disruptions_url,
        {},
        updateHtml);

    callback();

}
