/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;
import java.util.UUID;
import water.*;

/**
 *
 * @author peta
 */
public class PutValue extends H2OPage {

  @Override protected String serve_impl(Properties args) {
    String key_s = args.getProperty("Key",UUID.randomUUID().toString());
    if (key_s.isEmpty())
      key_s = UUID.randomUUID().toString();
    String val_s = args.getProperty("Value");
    int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if ((rf<0) || (rf>127))
      return error("Replication factor must be from 0 to 127.");
    Key key;
    try { 
      key = Key.make(key_s,(byte)rf);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return error("Not a valid key: "+ key_s);
    }
    Value val = new Value(key, val_s);
    // Insert in store
    UKV.put(key,val);
    RString response = new RString(html);
//    response.clear();
    response.replace("key",key_s);
    response.replace("rf",rf);
    response.replace("vsize",val_s.length());
    return response.toString();
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Value" };
  }
  
  public static final String html = 
            "<div class='alert alert-success'>"
          + "Key <strong>%key</strong> has been put to the store with replication factor %rf, value size <strong>%vsize</strong>."
          + "</div>"
          + "<p><a href='StoreView'><button class='btn btn-primary'>Back to Node</button></a>&nbsp;&nbsp;"
          + "<a href='Put'><button class='btn'>Put again</button></a>"
          + "</p>"
          ;
  
//  public static final RString response = new RString(html);

  
}
