package water.web;

import java.util.Properties;
import water.Key;
import water.UKV;

/**
 *
 * @author peta
 */
public class Remove extends H2OPage {
  
  @Override public String serve_impl(Properties args) {
    String keys = args.getProperty("Key");
    Key key = null;
    try { 
      key = Key.make(keys);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return H2OPage.error("Not a valid key: "+ keys);
    }
    // Distributed remove
    UKV.remove(key);
    // HTML file save of Value
    return H2OPage.success("Removed key <strong>"+keys+"</strong>");
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
