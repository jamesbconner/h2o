package water.web;

import java.util.Properties;
import water.Key;
import water.UKV;

/**
 *
 * @author peta
 */
public class Remove extends H2OPage {
  
  @Override public String serveImpl(Server server, Properties args) {
    Object o = ServletUtil.check_key(args,"Key");
    if( o instanceof String ) return (String)o;
    // Distributed remove
    UKV.remove((Key)o);
    // HTML file save of Value
    return H2OPage.success("Removed key <strong>"+((Key)o).toString()+"</strong>");
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
