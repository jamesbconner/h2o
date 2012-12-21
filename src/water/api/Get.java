
package water.api;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Properties;
import water.DKV;
import water.Key;
import water.NanoHTTPD;
import water.Value;
import water.web.H2OPage;
import water.web.Page;
import water.web.Server;

/**
 *
 * @author peta
 */
public class Get extends Request {

  protected H2OExistingKey _key = new H2OExistingKey(JSON_KEY);


  @Override public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    switch (type) {
      case help:
        return wrap(server,serveHelp());
      case wiki:
        return wrap(server,serveWiki());
      case json:
        JsonObject resp = new JsonObject();
        resp.addProperty(JSON_ERROR,"This request is only provided for browser connections");
        return wrap(server, resp);
      case www:
        String query = checkArguments(args, type);
        if (query != null)
          return wrap(server,query,type);
        // do the get
        return serve(server);
      case query:
        query = checkArguments(args, type);
        return wrap(server,query);
      default:
        throw new RuntimeException("Invalid request type "+type.toString());
    }
  }

  @Override protected Response serve() {
    throw new Error("NOT IMPLEMENTED YET");
  }

  protected NanoHTTPD.Response serve(NanoHTTPD server) {
    try {
      Value val = _key.value();
      Key key = val._key;
      if (!key.user_allowed())
        return wrap(server,build(Response.error("Not a user key: " + key)));
      // HTML file save of Value
      NanoHTTPD.Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY,val.openStream());
      res.addHeader("Content-Length", Long.toString(val.length()));
      res.addHeader("Content-Disposition", "attachment; filename="+key.toString());
      return res;
    } catch (Exception e) {
      return wrap(server,build(Response.error(e.getMessage())));
    }
  }



}
