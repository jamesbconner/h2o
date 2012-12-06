
package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.Key;
import water.Value;

/**
 *
 * @author peta
 */
public class PutValue extends Request {

  public static final String JSON_KEY = "key";
  public static final String JSON_VALUE = "value";
  public static final String JSON_RF = "rf";

  protected final KeyArgument _key = new KeyArgument(JSON_KEY);
  protected final StringArgument _value = new StringArgument(JSON_VALUE);
  protected final IntegerArgument _rf = new IntegerArgument(JSON_RF,0,256,2);

  @Override
  public void serve(JsonObject response, Properties args) {
    Key k = _key.value(args);
    Value v = new Value(k,_value.value(args).getBytes(),)
  }

}
