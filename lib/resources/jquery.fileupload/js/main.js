$(function () {
    $('#Fileupload').fileupload({
    	url: 'PutFile.json',
    	type: 'POST',        
        sequentialUploads: true,
//        maxChunkSize: 1000, // uncomment to enable chunked uploads - supported only in FF&Chrome
        change: function (e, data) {    	  
    	  $.each(data.files, function (index, file) {
    		  $('#filename').text(file.name);
    		  $('#filesize').text(file.size +'B');
    	    });    	  
    	  resetUploadTable();
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
                        	  uri = uri + ':' + result.port;
                        	  data.url = uri;                      	
                        	  data.submit();
                    	  }
                      });
              });
        },
        stop: function (e, data) {
        	$('#UploadBtn').text("Done!").addClass("btn-success");            
        },
        fail: function (e, data) {
        	$('#UploadBtn').text("Failed!").addClass("btn-danger");
        	console.log("FAIL");
        	console.log(data.textStatus);
        	console.log(data.errorThrown);
        	console.log(JSON.stringify(data));        	
        },        
    });    
        
    $('#UploadTable').hide();
    
    function resetUploadTable() {
    	$('#progress .bar').css('width','0%');
    	$('#UploadBtn').text("Upload");
    	if ($('#UploadBtn').hasClass("btn-success")) $('#UploadBtn').removeClass("btn-success");
    	if ($('#UploadBtn').hasClass("btn-danger")) $('#UploadBtn').removeClass("btn-danger");
    }
});

