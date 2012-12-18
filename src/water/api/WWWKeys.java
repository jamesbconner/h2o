
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


  private final Str _filter = new Str(JSON_FILTER,"");
  private final Int _limit = new Int(JSON_LIMIT,1024,0,10240);

  public WWWKeys() {
    _requestHelp = "Provides a simple JSON array of filtered keys known to the "
            + "current node.";
    _filter._requestHelp = "Only keys whose names contain the given filter will be"
            + " returned.";
    _limit._requestHelp = "Max number of key names to be returned. If the number"
            + " of keys to be returned is greater than this number, an empty list"
            + " is returned with an error and larger filter must be specified.";
  }

  @Override protected Response serve() {
    JsonArray array = new JsonArray();
    Key[] keys = new Key[_limit.value()];    // Limit size of what we'll display on this page
    int len = 0;
    String filter = _filter.value();
    //PersistHdfs.refreshHDFSKeys();
    // Gather some keys that pass all filters
    for( Key key : H2O.keySet() ) {
      if( filter != null &&     // Have a filter?
          key.toString().indexOf(filter) == -1 )
        continue;               // Ignore this filtered-out key
      if( !key.user_allowed() ) // Also filter out for user-keys
        continue;
      if( H2O.get(key) == null ) continue; // Ignore misses
      keys[len++] = key;        // Capture the key
      if( len == keys.length )
        return Response.error("Too many keys for given options");
    }
    // sort the keys, for pretty display & reliable ordering
    Arrays.sort(keys,0,len);
    for (int i = 0; i < len; ++i)
      array.add(new JsonPrimitive(keys[i].toString()));
    JsonObject response = new JsonObject();
    response.add(JSON_KEYS,array);
    return Response.done(response);
  }

}
