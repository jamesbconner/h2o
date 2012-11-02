
package water.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.util.Properties;
import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;
import water.exec.VAIterator;

/**
 *
 * @author peta
 */
public class GetVector extends JSONPage {
  
  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    try {
      int maxItems = (int) Double.parseDouble(parms.getProperty("MaxItems","200000")); // we need this because R uses e+ format even for integers
      Value v = DKV.get(Key.make(parms.getProperty("Key")));
      if (v==null)
        throw new IOException("Key not found");
      if (!(v instanceof ValueArray))
        throw new IOException("Only ValueArrays can be returned at this point");
     
      long startRow = Long.parseLong(parms.getProperty("startRow","0"));
      
      
      VAIterator iter = new VAIterator(Key.make(parms.getProperty("Key")), 0, startRow);
      
      long rows = Long.parseLong(parms.getProperty("rows",String.valueOf(Math.min(iter._ary.num_rows()-startRow,maxItems / iter._ary.num_cols()))));
      JsonArray columns = new JsonArray();
      JsonArray[] cols = new JsonArray[iter._ary.num_cols()];
      for (int i = 0; i < cols.length; ++i) 
        cols[i] = new JsonArray();
      for (int j = 0; j < rows; ++j) {
        iter.next();
        for (int i = 0 ; i < cols.length; ++i) {
          cols[i].add(new JsonPrimitive(iter.datad(i)));
        }
      }
      for (int i = 0; i < cols.length; ++i) {
        JsonObject col = new JsonObject();
        String name = iter._ary.col_name(i);
        col.addProperty("name", (name == null || name.isEmpty()) ? String.valueOf(i) : name);
        col.add("contents",cols[i]);
        columns.add(col);
      }
      result.addProperty("key",v._key.toString());
      result.add("columns",columns);
      result.addProperty("num_rows",iter._ary.num_rows());
      result.addProperty("num_cols",iter._ary.num_cols());
      result.addProperty("sent_rows",rows);
    } catch (Exception e) {
      result.addProperty("Error", e.toString());
    }  
    return result;
  }
    
  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
  

}



