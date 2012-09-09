package water.web;

import java.util.Properties;

public abstract class Page {

  public String[] requiredArguments() {
    return null;
  }

  public abstract Object serve(Server server, Properties args);
}
