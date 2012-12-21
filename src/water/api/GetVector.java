
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import water.Key;
import water.ValueArray;
import water.exec.VAIterator;
import water.web.ServletUtil;

/**
 *
 * @author peta
 */
public class GetVector extends JSONOnlyRequest {

  public static int MAX_REQUEST_ITEMS = 200000;

  public static final String JSON_MAX_ROWS = "max_rows";
  public static final String JSON_NAME = "name";
  public static final String JSON_CONTENTS = "contents";
  public static final String JSON_NUM_ROWS = "num_rows";
  public static final String JSON_NUM_COLS = "num_cols";
  public static final String JSON_SENT_ROWS = "sent_rows";



  protected H2OHexKey _key = new H2OHexKey(JSON_KEY);
  protected Int _maxRows = new Int(JSON_MAX_ROWS,Integer.MAX_VALUE);

  @Override
  protected Response serve() {
    JsonObject result = new JsonObject();
    try {
      ValueArray va = _key.value();

      VAIterator iter = new VAIterator(va._key, 0, 0);

      long maxRows = Math.min(MAX_REQUEST_ITEMS / iter._ary.numCols(), iter._ary.numRows());



      maxRows = Math.min(_maxRows.value(),maxRows);  // we need this because R uses e+ format even for integers

      JsonArray columns = new JsonArray();
      JsonArray[] cols = new JsonArray[iter._ary.numCols()];
      for (int i = 0; i < cols.length; ++i)
        cols[i] = new JsonArray();
      for (int j = 0; j < maxRows; ++j) {
        iter.next();
        for (int i = 0 ; i < cols.length; ++i) {
          if (iter.isValid(i))
            cols[i].add(new JsonPrimitive(String.valueOf(iter.datad(i))));
          else
            cols[i].add(new JsonPrimitive("NaN"));
        }
      }
      for (int i = 0; i < cols.length; ++i) {
        JsonObject col = new JsonObject();
        String name = iter._ary._cols[i]._name;
        col.addProperty(JSON_NAME, (name == null || name.isEmpty()) ? String.valueOf(i) : name);
        col.add(JSON_CONTENTS,cols[i]);
        columns.add(col);
      }
      result.addProperty(JSON_KEY,va._key.toString());
      result.add(JSON_COLUMNS,columns);
      result.addProperty(JSON_NUM_ROWS,iter._ary.numRows());
      result.addProperty(JSON_NUM_COLS,iter._ary.numCols());
      result.addProperty(JSON_SENT_ROWS,maxRows);
    } catch (Exception e) {
      return Response.error(e.toString());
    }
    return Response.done(result);
  }

}
