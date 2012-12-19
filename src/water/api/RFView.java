
package water.api;

import com.google.gson.JsonObject;

/**
 *
 * @author peta
 */
public class RFView extends Request {

  @Override protected Response serve() {




    return Response.done(new JsonObject());
  }

}
