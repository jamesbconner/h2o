/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;

import water.Key;
import water.UKV;

/**
 *
 * @author peta
 */
public class Append extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args) {
    RString response = new RString(html);
    String key_s = args.getProperty("Key");
    String append = args.getProperty("Append","");
    if (append.isEmpty())
      return error("Appending an empty string does not change the value for key <strong>"+key_s+"</strong>");
    Key key = null;
    try { 
      key = Key.make(key_s);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return error("Not a valid key: "+ key_s);
    }
    UKV.append(key,append.getBytes());
    //response.clear();
    response.replace("key",key_s);
    return response.toString();
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
  
  public static final String html = 
            "<div class='alert alert-success'>"
          + "Value of key <strong>%key</strong> has been appended by the given text"
          + "</div>"
          + "<p><a href='StoreView'><button class='btn btn-primary'>Back to Node</button></a>&nbsp;&nbsp;"
          + "<a href='AppendQuery'><button class='btn'>Append again</button></a>"
          + "</p>"
          ;
  
//  public static final RString response = new RString(html);
  
}
