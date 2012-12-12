
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class WWWHexKeyCols extends Request {

  public final H2OHexKey _key = new H2OHexKey(JSON_KEY,"Hex key");

  @Override protected void serve(JsonObject response) {
    ValueArray va = _key.value();
    JsonArray ary = new JsonArray();
    for (int i = 0; i < va.num_cols(); ++i)
      ary.add(new JsonPrimitive(va.col_name(i)));
    response.add(JSON_COLUMNS,ary);
  }



}
