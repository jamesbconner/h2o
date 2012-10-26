
package water.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.util.Properties;
import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class GetVector extends JSONPage {
  
  @Override public JsonObject serverJson(Server server, Properties parms, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    try {
      Value v = DKV.get(Key.make(parms.getProperty("Key")));
      if (v==null)
        throw new IOException("Key not found");
      if (!(v instanceof ValueArray))
        throw new IOException("Only ValueArrays can be returned at this point");
      ValueArray va = (ValueArray) v;
      if (va.num_rows()*va.num_cols() > 200000)
        throw new IOException("Only dataframes with less than 2000000 entries (cols*rows) are supported now");
      JsonArray columns = new JsonArray();
      for (int i = 0; i < va.num_cols(); ++i) {
        JsonObject col = new JsonObject();
        if ((va.col_name(i) == null) || (va.col_name(i).isEmpty()))
          col.addProperty("name",i);
        else 
          col.addProperty("name",va.col_name(i));
        StringBuilder sb = new StringBuilder();
        sb.append(va.datad(0,i));
        for (int j = 1; j < va.num_rows(); ++j) {
          sb.append(" ");
          sb.append(va.datad(j,i));
        }
        col.addProperty("contents",sb.toString());
        columns.add(col);
      }
      result.addProperty("key",v._key.toString());
      result.add("columns",columns);
      result.addProperty("num_rows",va.num_rows());
      result.addProperty("num_cols",va.num_cols());
    } catch (Exception e) {
      result.addProperty("Error", e.toString());
    }  
    return result;
  }
    
  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
  

}



