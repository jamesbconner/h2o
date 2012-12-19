
package water.api;

import com.google.gson.JsonObject;
import water.Key;
import water.Value;
import water.parser.ParseDataset;

/**
 *
 * @author peta
 */
public class Parse extends Request {

  public static final String JSON_SOURCE_KEY = "source";
  public static final String JSON_DEST_KEY = "dest";

  protected final H2OExistingKey _source = new H2OExistingKey(JSON_SOURCE_KEY);
  protected final H2OKey _dest = new H2OKey(JSON_DEST_KEY, (Key)null);

  private KeyElementBuilder KEY_BUILDER = new KeyElementBuilder();

  @Override protected Response serve() {
    Value source = _source.value();
    Key dest = _dest.value();
    if (dest == null)
      dest = Key.make(source._key.toString()+".hex");
    try {
      ParseDataset.parse(dest, source);
      JsonObject response = new JsonObject();
      response.addProperty(JSON_DEST_KEY,dest.toString());
      Response r = Response.done(response);
      r.setBuilder(JSON_DEST_KEY,KEY_BUILDER);
      return r;
    } catch (IllegalArgumentException e) {
      return Response.error(e.getMessage());
    } catch (Error e) {
      return Response.error(e.getMessage());
    }
  }

}
