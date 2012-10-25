package water.web;
import com.google.gson.JsonObject;
import java.util.Properties;
import water.Key;
import water.exec.PositionedException;

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
    JsonObject res = new JsonObject();
    try {
      Key k = water.exec.Exec.exec(x);
      res.addProperty("Expr", x);
      addProperty(res,"ResultKey", k);
    } catch( PositionedException e ) {
      res.addProperty("Expr", x);
      res.addProperty("Error", e.report(x));      
    }
    return res;
  }

  //
  @Override protected String serveImpl(Server server, Properties args, String sessionId) throws PageError {
    RString query = new RString(ExecQuery.html);
    query.replace("expr",args.getProperty("Expr"));
    try {
      Key k = water.exec.Exec.exec(args.getProperty("Expr"));
      args.put("Key",k.toString());
      return query.toString() + new Inspect().serveImpl(server,args,sessionId);
    } catch (PositionedException e) {
      return query.toString() + error("<span style='font-family:monospace'>"+e.reportHTML(args.getProperty("Expr")) +"</span>");
    }
  }
}
