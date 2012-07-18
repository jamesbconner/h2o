package water.web;

import java.util.Properties;
import water.DKV;
import water.Key;
import water.NanoHTTPD.Response;
import water.NanoHTTPD;
import water.Value;

/**
 *
 * @author peta
 */
public class Get extends Page {

  @Override public Object serve(Server server, Properties args) {
    String key_s = args.getProperty("Key");
    Key key = null;
    try { 
      key = Key.make(key_s);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return H2OPage.wrap(H2OPage.error("Not a valid key: "+ key_s));
    }
    if (!key.user_allowed())
      return H2OPage.wrap(H2OPage.error("Not a user key: "+ key.toString()));    
    // Distributed get
    Value val = DKV.get(key);

    if( val == null )
      return H2OPage.wrap(H2OPage.error("Key not found: "+ key_s));
    // HTML file save of Value
    Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY,val.openStream());
    res.addHeader( "Content-Length", Long.toString(val.length()));
    return res;
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
