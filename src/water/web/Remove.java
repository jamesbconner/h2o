package water.web;

import java.util.Properties;

import water.Key;
import water.UKV;

public class Remove extends H2OPage {

  @Override public String serveImpl(Server server, Properties args) throws PageError {
    Key key = ServletUtil.check_key(args,"Key");
    UKV.remove(key);
    return H2OPage.success("Removed key <strong>"+encode(key)+"</strong>");
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
