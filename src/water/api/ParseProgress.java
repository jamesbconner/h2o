
package water.api;

import water.*;
import water.parser.ParseStatus;

import com.google.gson.JsonObject;

public class ParseProgress extends Request {
  protected final H2OExistingKey _dest = new H2OExistingKey(DEST_KEY);

  public static Response redirect(JsonObject fromPageResponse, Key dest) {
    JsonObject destPageParams = new JsonObject();
    destPageParams.addProperty(DEST_KEY, dest.toString());
    return Response.redirect(fromPageResponse, ParseProgress.class, destPageParams);
  }

  @Override protected Response serve() {
    Value v = _dest.value();
    JsonObject response = new JsonObject();
    response.addProperty(RequestStatics.DEST_KEY, v._key.toString());
    Response r;
    if( v._isArray == 1 ) {
      r = Inspect.redirect(response, v._key);
    } else {
      ParseStatus ps = v.get(new ParseStatus());
      r = Response.poll(response, (float) ps.getProgress());
    }
    r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
    return r;
  }

}
