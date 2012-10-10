package water.web;
import com.google.gson.*;
import java.util.*;
import water.Key;
import water.UKV;
import water.Value;
import water.exec.*;

/**
 * The servlet for launching a computation
 *
 * @author cliffc@0xdata.com
 */
public class ExecWeb extends H2OPage {
  @Override
  public JsonObject serverJson(Server server, Properties args, String sessionID) throws PageError {
    // Get parameters: Key, file name, replication factor
    String x = args.getProperty("Expr");
    if( x==null || x.isEmpty() ) throw new PageError("Expression is missing");

    Key k = water.exec.Exec.exec(x);

    JsonObject res = new JsonObject();
    res.addProperty("Expr", x);
    res.addProperty("ResultKey", k.toString());
    return res;
  }

  //
  @Override protected String serveImpl(Server server, Properties args, String sessionId) throws PageError {
    JsonObject json = serverJson(server, args, sessionId);
    args.put("Key",encode(Key.make(json.getAsJsonPrimitive("ResultKey").getAsString())));
    return new Inspect().serveImpl(server,args,sessionId);
  }
}
