/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;

import water.Key;
import water.ValueCode;

/**
 *
 * @author peta
 */
public class Exec extends H2OPage {
  
  public static String execute(Key key, String args) {
    String[] args2 = args.isEmpty() ? new String[0] : args.split("&");
    String result = ValueCode.exec(key,args2);
    RString response = new RString(html);
//    response.clear();
    response.replace("key",key.toString());
    response.replace("result",result);
    return response.toString();
  } 
  
  @Override protected String serve_impl(Properties args) {
    // Source Key: Value should be jar file bytes
    String key_s = args.getProperty("Key");
    
    Key src;
    try {
      src = Key.make(key_s);
    } catch( IllegalArgumentException e ) {
      return error("Not a valid key: "+ key_s);
    }
    String res = execute(src,args.getProperty("Args",""));
    return res;
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
  
  public static final String html = 
            "<div class='alert alert-success'>"
          + "Key %key has been executed with the following result:"
          + "</div>"
          + "<pre>%result</pre>"
          + "<p><a href='StoreView'><button class='btn'>Back to Node</button></a>&nbsp;&nbsp;"
          + "<a href='ExecQuery'><button class='btn'>Execute Another</button></a>&nbsp;&nbsp;"
          + "</p>"
          ;
  
//  public static final RString response = new RString(html);
  
}
