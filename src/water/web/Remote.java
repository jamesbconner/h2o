package water.web;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import water.DKV;
import water.H2O;
import water.H2ONode;
import water.Key;
import water.TaskGetKeys;
import water.Value;
import water.web.Page.PageError;

public class Remote extends H2OPage {

  @Override protected String serveImpl(Server server, Properties args) throws PageError {
    RString response = new RString(html);
    // Figure out the remote Node's IP address.  We know the URI
    // string starts with '/Node=', but the rest of it should be an
    // IPV4 xx.xx.xx.xx:port style string.
    String addr = args.getProperty("Node");
    if( addr.charAt(0)=='/' ) addr = addr.substring(1);
    String[] adrs = addr.split(":");
    H2ONode h2o=null;
    try {
      h2o = H2ONode.intern(InetAddress.getByName(adrs[0]),Integer.decode(adrs[1]));
    } catch( UnknownHostException uhe ) {
      return error("Unknown address "+addr);
    } catch( NumberFormatException nfe ) {
      return error("Unknown port "+adrs[1]);
    }
    H2O cloud = H2O.CLOUD;
    if( h2o == null || !cloud._memset.contains(h2o) )
      return error("Unknown Node "+addr);
    if( h2o == H2O.SELF ) // Service self-node locally
      return ((H2OPage)Server.getPage("StoreView")).serveImpl(server, new Properties());

    // Ask the remote H2ONode for 'len' keys from *his* local store, after
    // skipping 'off' keys.  This is NOT ALL his keys, thats too much.  Some
    // Day this should open a Stream of keys.  No guarantees on order.
    Key[] keys = new TaskGetKeys(h2o,0,25).get();
    if( keys == null )
      return error("Unknown Node "+addr);

    // Build a click-able table of K/V pairs
    int alt=0;                  // Alternate color per row
    for( Key key : keys ) {     // For all keys returned
      Value val = DKV.get(key,100); // Do a distributed get (caching locally)
      if( val == null ) continue;   // Key was deleted before viewing
      RString row = response.restartGroup("tableRow");
      row.replace("key",key.toString());
      row.replace("value",val.getString(100));
      row.append();
      alt++;
    }
    response.replace("noOfKeys",alt);
    return response.toString();
  }

  final static String html =
      "<div class='alert alert-success'>Displaying keys at node <strong>%ip</strong></div>"
    + "<p>The Remote Store returned <strong>%noOfKeys</strong> keys</page>"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<thead><th>Key<th>Value</thead>"
    + "%tableRow{"
    + "  <tr>"
    + "    <td>%key</td>"
    + "    <td>%value</td>"
    + "  </tr>"
    + "}"
    ;

  @Override public String[] requiredArguments() {
    return new String[] { "Node" };
  }
}
