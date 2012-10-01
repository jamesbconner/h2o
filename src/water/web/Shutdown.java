package water.web;

import java.util.Properties;

import com.google.gson.JsonObject;

import water.*;

public class Shutdown extends H2OPage {
  @Override public JsonObject serverJson(Server server, Properties parms) throws PageError {
    UDPRebooted.global_kill();
    JsonObject json = new JsonObject();
    json.addProperty("Status", "Shutting down");
    return json;
  }

  @Override public String serveImpl(Server server, Properties args) {
    UDPRebooted.global_kill();
    return "Shutting down";
  }
}
