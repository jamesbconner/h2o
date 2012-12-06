
package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;

/**
 *
 * @author peta
 */
public class HTTP404 extends Request {
  public static final String NAME = "HTTP-404";

  private final StringArgument _error = new StringArgument(JSON_ERROR,"Unknown error");

  @Override public void serve(JsonObject response, Properties args) {
    response.addProperty(JSON_ERROR,_error.value(args));
    response.addProperty(JSON_ERROR_TYPE,NAME);
  }

  public String createHtml(JsonObject response) {
    StringBuilder sb = new StringBuilder();
    DOM.error(sb,"<h1>HTTP 404</h1><p>"+response.get(JSON_ERROR).getAsString()+"</p>");
    return sb.toString();
  }


}
