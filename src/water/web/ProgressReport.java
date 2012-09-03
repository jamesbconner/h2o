/**
 * 
 */
package water.web;

import java.util.Properties;

import water.Log;
import water.Log.LogListener;
import water.NanoHTTPD;
import water.NanoHTTPD.Response;

/**
 * @author michal
 *
 */
public class ProgressReport extends Page {
  
  private static final String NO_OUTPUT = "<no output>";
  
  // TODO optional parameter to get stdout or stderr

  @Override
  public Object serve(Server server, Properties args) {
    
    
    LogListener l = null;
    if (System.out instanceof Log) {
      l = ((Log) System.out).registerListener();
    }
    
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
    
    Response res = server.new Response(NanoHTTPD.HTTP_OK, "text/plain", l.getInputStream());
//    res.addHeader("Connection", "keep-alive");
    res.addHeader("Access-Control-Allow-Origin", "*");
    return res;
  }
  
}
