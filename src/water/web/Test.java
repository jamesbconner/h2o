package water.web;

import java.util.Properties;
import water.*;

/**
 *
 * @author cliffc
 */
public class Test extends H2OPage {
 
  public static String run_test(String test) {
    throw H2O.unimpl(); // nice idea but needs to use JUnit
    //try {
    //  Log.buffer_sys_out_err();
    //  DKV.remove(Driver.ERRKEY);
    //  Tester.processArguments(test);
    //  Tester.start();
    //  Value errlog = DKV.get(Driver.ERRKEY);
    //  if( errlog == null ) {
    //    System.out.println("no errors");
    //  }
    //} finally {
    //  return Log.unbuffer_sys_out_err().toString();
    //}
  }
  
  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    // Source Key: Value should be test class
    String key_s = args.getProperty("Name");
    String res = run_test(key_s);

    RString response = new RString(html);
    response.replace("key",key_s);
    response.replace("res",res);
    return response.toString();
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Name" };
  }
  
  public static final String html = 
            "<div class='alert alert-success'>"
          + "Test %key results: <p><pre>%res</pre></p>"
          ;

}
