package water.web;

import java.util.Properties;

import com.google.gson.JsonObject;

import water.*;

public class Shutdown extends H2OPage {
  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    UDPRebooted.global_kill(2);
    JsonObject json = new JsonObject();
    json.addProperty("Status", "Shutting down");
    return json;
  }

  @Override public String serveImpl(Server server, Properties args, String sessionID) {
    UDPRebooted.global_kill(2);
    System.exit(-1);
    return "Shutting down";
  }
}
