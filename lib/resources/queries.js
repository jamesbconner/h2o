
function query_checkArg(elementName) {
  $.get(REQUEST_ID+".checkArg", $("#query").serialize()+"?__arg__="+elementName, function(data) {
    if ("error" in data) {
      query_show_error(elementName, data.error);
    } else {
      $("#query").attr("action",REQUEST_ID+".query");
      $("#query").submit();
    }
  });
}



function query_show_error(elementName, message) {
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

function query_check(requestId, elementName) {
  var element = $("#"+elementName);
  var args = { __arg__ : elementName };
  args[elementName] = element.val();
  $.get(requestId+".checkArg", args, function (data) {
    if ("error" in data)
      query_show_error(elementName, data.error);
    else
      query_show_success(elementName, "Correct:)");
  });
}

// Clears the notice and displays the control
function query_enable(elementName) {
    
  
}

// Displays the message in the notice and disables the control
function query_disable(elementName, message) {
  
  
}
