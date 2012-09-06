/**
 * 
 */
package water.web;

import java.io.InputStream;
import java.util.Properties;

import water.LocalLogSubscriber;
import water.LogHub;
import water.NanoHTTPD;
import water.NanoHTTPD.Response;

/**
 * @author michal
 *
 */
public class ProgressReport extends Page {
  
  @Override
  public Object serve(Server server, Properties args) {
    
    LocalLogSubscriber lls = new LocalLogSubscriber();    
    LogHub.subscribe(null, lls);        
    
    System.out.println("ProgressReport.serve(): " + Thread.currentThread().getName());
        
    System.out.println("Thread starting()");
    
    Thread t = new Thread(new Runnable() {      
      @Override
      public void run() {
        int counter = 0;
        while (++counter < 50) {
          System.out.println("Hi #"+counter+ " there from " + Thread.currentThread().getName()); 
          try {
            Thread.sleep(1000);
          } catch( InterruptedException e ) {
            throw new RuntimeException(e);          
          }
        }
      }
    });
    t.start();
    
    InputStream is = lls.getInputStream();        
    Response res = server.new Response(NanoHTTPD.HTTP_OK, "text/plain", is);
    // res.addHeader("Connection", "keep-alive");
    res.addHeader("Access-Control-Allow-Origin", "*");
    return res;
  }
  
}
