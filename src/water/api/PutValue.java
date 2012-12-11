
package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.H2O;
import water.Key;
import water.UKV;
import water.Value;

/**
 *
 * @author peta
 */
public class PutValue extends Request {

  public static final String HTTP_KEY = "key";
  public static final String HTTP_VALUE = "value";
  public static final String HTTP_RF = "rf";

  public static final String JSON_KEY = "key";
  public static final String JSON_VALUE_SIZE = "value_size";
  public static final String JSON_RF = "rf";

  protected final H2OKey _key = new H2OKey(HTTP_KEY,"Key");
  protected final Str _value = new Str(HTTP_VALUE,"Value");
  protected final Int _rf = new Int(HTTP_RF,2,"Replication factor",0,255);

  @Override public void serve(JsonObject response) {
    Key k = Key.make(_key.value()._kb, (byte) (int)_rf.value());
    Value v = new Value(k,_value.value().getBytes());
    UKV.put(k,v);
    response.addProperty(JSON_KEY,k.toString());
    response.addProperty(JSON_RF,k.desired());
    response.addProperty(JSON_VALUE_SIZE,v._max);
  }

}
