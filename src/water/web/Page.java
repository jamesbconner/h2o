package water.web;

import java.util.Properties;

import com.google.gson.JsonElement;

public abstract class Page {

  public String[] requiredArguments() {
    return null;
  }

  public abstract Object serve(Server server, Properties args);

  public JsonElement serverJson(Server server, Properties parms) {
    return null;
  }
}
