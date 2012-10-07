$(function () {
    $('#Fileupload').fileupload({
    	url: 'Upload',
    	type: 'POST',        
        sequentialUploads: true,
        dataType: 'json',
//        maxChunkSize: 1000, // uncomment to enable chunked uploads - supported only in FF&Chrome
        change: function (e, data) {
          resetUploadTable();
    	  $.each(data.files, function (index, file) {
    		  $('#filename').text(file.name);
    		  if (file.size === undefined)
    			  $('#filesize').text('N/A B');
    		  else 
    			  $('#filesize').text(file.size +' B');
    	    });
    	  $('#UploadTable').show();    	  
        },
        progressall: function (e, data) {
            var progress = parseInt(data.loaded / data.total * 100, 10);
            $('#progress .bar').css('width',progress + '%');          
        },        
        add: function (e, data) {          
          $('#UploadBtn').prop("disabled",false);
          $('#UploadBtn').unbind('click').click(function(e) {    	
              	e.preventDefault();    	
              	$(this).prop("disabled",true);
              	$.getJSON('PutFile.json', 
              	    { File: $('table #filename').text(), Key: $("table #Key").val(), RF: $("table #RF").val() },
                      function(result){
                    	  if (result) {
                        	  var uri = document.URL;
                        	  if (uri.lastIndexOf(':') > 4) 
                        		  uri = uri.slice(0,uri.lastIndexOf(':'))
                        	  else 
                        		  uri = uri.slice(0,uri.lastIndexOf('/'))
                        	  uri = uri + ':' + result.port + '/Upload';
                        	  data.url = uri;                      	
                        	  data.submit();
                    	  }
                      });
              });
        },
        done: function (e, data) {        	
        	var result = data.result;
        	if (result.constructor == String) { // IE returns string, however Chrome, Safari do automatic parsing
        		result = $.parseJSON(result);
        	}
        	if ($.isArray(result)) { 
        		if (result[0]) {
                	result = result[0];
                	$('#progress .bar').css('width','100%');
                	$('table #filename').html("<a href='Get?Key=" + result.keyHref +"'>" + result.name + "</a>");
                	$('table #keytd').html("<span class='span3'><a href='Inspect?Key=" + result.keyHref +"'>" + result.key + "</a></span>");
                	$('table #rftd').html("<span class='span2'>" + result.rf + "</html>");
                	$('table #btntd').html("<span class='label label-success'>DONE</span>");
                	
                	return;
        		}
        	} 
        	// ups something is wrong
        	failedUpload("callback done(), but the response is wrong: " + data.response, data);
        },
        fail: function (e, data) {
        	failedUpload("callback fail()", data);
        },        
    });    
            
    function resetUploadTable() {
    	$('#UploadTable').empty();
    	$('#UploadTable').append("<tr><td class='span2' id='filename'></td>"
        + "      <td class='span2' id='filesize'></td>"
        + "      <td class='span3' id='keytd'><input type='text' class='input-small span2' placeholder='key (optional)' name='Key' id='Key' maxlength='512'></td>"
        + "      <td class='span2' id='rftd'><input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'></td>"
        + "      <td class='span6'><div id='progress' class='progress progress-striped span6'><div class='bar' style='width: 0%;'></div></div></td>"
        + "      <td class='span1' id='btntd'><button type='submit' class='btn btn-primary' id='UploadBtn'>Upload</button></td>"
        + "  </tr>");
    }
    function failedUpload(msg,data) {
    	$('#UploadBtn').text("Failed!").addClass("btn-danger");
    	console.log("FAILED upload: " + msg);
    	console.log(data.textStatus);
    	console.log(data.errorThrown);    	
    }
    
    $('#UploadTable').hide();
});

