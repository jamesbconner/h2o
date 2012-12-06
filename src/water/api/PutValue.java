
package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;

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
  protected final IntegerArgument _rf = new IntegerArgument(JSON_RF,0,10,2);

  @Override
  public void serve(JsonObject response, Properties args) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
