function query_show_error(elementName, message) {
  query_hide_success(elementName);
  var element = $("#inputQuery_error_"+elementName);
  element.html(message);
  element.css("display","block");
}

function query_hide_error(elementName) {
  var element = $("#inputQuery_error_"+elementName);
  element.css("display","none");
}

function query_show_success(elementName, message) {
  query_hide_error(elementName);  
  var element = $("#inputQuery_success_"+elementName);
  element.html(message);
  element.css("display","block");
}

function query_hide_success(elementName) {
  var element = $("#inputQuery_success_"+elementName);
  element.css("display","none");
}
