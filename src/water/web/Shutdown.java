package water.web;

import java.util.Properties;
import java.util.TimerTask;

import water.UDPRebooted;

import com.google.gson.JsonObject;

public class Shutdown extends H2OPage {
  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    java.util.Timer t = new java.util.Timer("Shutdown Timer");
    t.schedule(new TimerTask() {
      @Override
      public void run() {
        UDPRebooted.global_kill(2);
        System.exit(-1);
      }
    }, 100);


    JsonObject json = new JsonObject();
    json.addProperty("Status", "Shutting down");
    return json;
  }

  @Override public String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    serverJson(server, args, sessionID);
    return "Shutting down";
  }


}
