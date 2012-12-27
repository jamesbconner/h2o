
package water.api;

import com.google.gson.JsonObject;
import water.Key;
import water.Value;
import water.parser.ParseDataset;

public class Parse extends Request {
  protected final H2OExistingKey _source = new H2OExistingKey(RequestStatics.JSON_SOURCE_KEY);
  protected final H2OKey _dest = new H2OKey(RequestStatics.JSON_DEST_KEY, (Key)null);

  @Override protected Response serve() {
    Value source = _source.value();
    Key dest = _dest.value();
    if (dest == null)
      dest = Key.make(source._key.toString()+".hex");
    try {
      ParseDataset.parse(dest, source);
      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.JSON_DEST_KEY,dest.toString());
      Response r = Response.done(response);
      r.setBuilder(RequestStatics.JSON_DEST_KEY, new KeyElementBuilder());
      return r;
    } catch (IllegalArgumentException e) {
      return Response.error(e.getMessage());
    } catch (Error e) {
      return Response.error(e.getMessage());
    }
  }

}
