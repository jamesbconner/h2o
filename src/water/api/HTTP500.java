
package water.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Properties;

/**
 *
 * @author peta
 */
public class HTTP500 extends Request {

  private final Str _error = new Str(JSON_ERROR,"Unknown error");

  @Override public Response serve() {
    return Response.error(_error.value());
  }


}
