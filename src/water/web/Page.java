package water.web;

import java.util.Properties;

import com.google.gson.JsonObject;

public abstract class Page {
  protected static class PageError extends Exception {
    public final String _msg;
    public PageError(String msg) { _msg = msg; }
  }

  public String[] requiredArguments() {
    return null;
  }

  public abstract Object serve(Server server, Properties args);

  public JsonObject serverJson(Server server, Properties parms) throws PageError {
    return null;
  }
}
