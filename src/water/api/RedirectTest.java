
package water.api;

import com.google.gson.JsonObject;

/**
 *
 * @author peta
 */
public class RedirectTest extends Request {

  @Override protected Response serve() {
    JsonObject resp = new JsonObject();
    resp.addProperty("hoho","hehe");
    return Response.redirect(resp,"PollTest",resp);
  }

}
