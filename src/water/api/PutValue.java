
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

  protected final KeyArgument _key = new KeyArgument(HTTP_KEY);
  protected final StringArgument _value = new StringArgument(HTTP_VALUE);
  protected final IntegerArgument _rf = new IntegerArgument(HTTP_RF,0,256,2);

  @Override public void serve(JsonObject response, Properties args) {
    Key k = Key.make(_key.value(args)._kb, (byte) (int)_rf.value(args));
    Value v = new Value(k,_value.value(args).getBytes());
    UKV.put(k,v);
    response.addProperty(JSON_KEY,k.toString());
    response.addProperty(JSON_RF,k.desired());
    response.addProperty(JSON_VALUE_SIZE,v._max);
  }

}
