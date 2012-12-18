
package water.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Properties;
import water.web.RString;

/**
 *
 * @author peta
 */
public class HTTP404 extends Request {

  private final Str _error = new Str(JSON_ERROR,"Unknown error");

  @Override public Response serve() {
    return Response.error(_error.value());
  }

  private static final String _html =
            "<h3>HTTP 404 - Not Found</h3>"
          + "<div class='alert alert-error'>%ERROR</div>"
          ;

  @Override protected String build(Response response) {
    RString str = new RString(_html);
    str.replace("ERROR", response.error());
    return str.toString();
  }

}
