package water.web;

import java.io.InputStream;
import java.util.Properties;

import water.LocalLogSubscriber;
import water.LogHub;
import water.NanoHTTPD;
import water.NanoHTTPD.Response;

/**
 * ProgressReport provide REST API to stream stdout/stderr of all nodes in the cloud.
 * 
 * @author michal
 *
 */
public class ProgressReport extends Page {
  
  @Override
  public Object serve(Server server, Properties args) {
   
    // Register a new local subscriber. 
    LocalLogSubscriber lls = new LocalLogSubscriber();    
    LogHub.subscribe(null, lls);
    
    // Return response with input stream providing access to a stream containing stdout/stderr.
    InputStream is = lls.getInputStream();        
    Response res = server.new Response(NanoHTTPD.HTTP_OK, "text/plain", is);
    // res.addHeader("Connection", "keep-alive");
    res.addHeader("Access-Control-Allow-Origin", "*");
    return res;
  }
  
}
