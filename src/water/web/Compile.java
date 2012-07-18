/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;
import water.*;

/**
 *
 * @author peta
 */
public class Compile extends H2OPage {
 
  public static Object compile(String key_s) {
    Key src = null;
    try {
      src = Key.make(key_s);
    } catch( IllegalArgumentException e ) {
      return error("Not a valid key: "+ key_s);
    }
    // Raw unverified jarfile bytes
    Value klass = DKV.get(src);
    if( klass == null )
      return error("Key "+src+" not mapped to anything");

    // Compile it
    Object res = ValueCode.compile(src);
    if( res instanceof String )
      return error("Error compiling "+src+"\n<p>"+res);

    return res;
  }
  
  @Override protected String serve_impl(Properties args) {
    // Source Key: Value should be jar file bytes
    String key_s = args.getProperty("Key");
    Object res = compile(key_s);
    if (res instanceof String)
      return (String)res;

    Key obj = (Key)res;
    assert DKV.get(obj) instanceof ValueCode;

    RString response = new RString(html);
//    response.clear();
    response.replace("key",key_s);
    response.replace("ckey",obj.toString());
    response.replace("ekey",obj.toString());
    return response.toString();
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
  
  public static final String html = 
            "<div class='alert alert-success'>"
          + "Key %key has been compiled to key <strong>%ckey</strong>"
          + "</div>"
          + "<p><a href='StoreView'><button class='btn'>Back to Node</button></a>&nbsp;&nbsp;"
          + "<a href='Put'><button class='btn'>Compile Another</button></a>&nbsp;&nbsp;"
          + "<a href='ExecQuery?Key=%ekey'><button class='btn btn-primary'>Execute This!</button></a>"
          + "</p>"
          ;
  
//  public static final RString response = new RString(html);
  
}
