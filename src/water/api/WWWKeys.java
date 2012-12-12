
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Arrays;
import water.H2O;
import water.Key;
import water.hdfs.PersistHdfs;

/**
 *
 * @author peta
 */
public class WWWKeys extends Request {

  public static final String JSON_FILTER = "filter";
  public static final String JSON_KEYS = "keys";
  public static final String JSON_LIMIT = "limit";

  private final Str _filter = new Str(JSON_FILTER,"","Filter for the keys to return");
  private final Int _limit = new Int(JSON_LIMIT,1024,"Max number of keys to return",0,10240);

  @Override protected void serve(JsonObject response) {
    JsonArray array = new JsonArray();
    Key[] keys = new Key[_limit.value()];    // Limit size of what we'll display on this page
    int len = 0;
    String filter = _filter.value();
    PersistHdfs.refreshHDFSKeys();
    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null &&     // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue;               // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key;        // Capture the key
      if( len == keys.length ) {
        response.addProperty(JSON_ERROR,"Too many keys for given options");
        array.add(new JsonPrimitive(""));
        response.add(JSON_KEYS,array);
        return;
      }
    }
    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    for (int i = 0; i < len; ++i)
      array.add(new JsonPrimitive(keys[i].toString()));
    response.add(JSON_KEYS,array);
  }

}
