
package water.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Properties;

/**
 *
 * @author peta
 */
public class HTTP404 extends Request {

  private final Str _error = new Str(JSON_ERROR,"Unknown error");

  @Override public void serve(JsonObject response) {
    response.addProperty(JSON_ERROR,_error.value());
  }

}
