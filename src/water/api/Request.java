
package water.api;

import com.google.gson.JsonObject;
import java.util.Properties;
import water.DKV;
import water.Key;
import water.Value;

/** A basic class for a JSON request.
 *
 *
 *
 * @author peta
 */
public abstract class Request {

  // override those to get the functionality required --------------------------

  /** Serves the given request and returns the resulting JSON object.
   *
   * @param args
   * @return
   */
  public abstract JsonObject serve(Properties args);


  /** Produces the HTML response from the given JSON object.
   *
   * In its default mode this method just displays the automatically generated
   * HTML page from the JSON object.
   *
   * If you want to redesign the page, overload this method.
   *
   * @param response
   * @return
   */
  public String createHtml(JsonObject response) {
    return "NOT IMPLEMENTED YET";
  }

  /** Creates the HTML query page for the request, if any.
   *
   * @param submittedArgs
   * @return
   */
  public String createQuery(Properties submittedArgs) {
    return "QUERY NOT IMPLEMENTED YET";
  }

  // argument checking ---------------------------------------------------------

  /** An argument to the request.
   *
   * The argument (its subclasses) provide a checked version of arguments of
   * different types being parsed to the request. Each request must register
   * its arguments beforehand in a static constructor.
   *
   * @param <T>
   */
  public abstract class Argument<T> {

    private final String _name;
    private final T _defaultValue;
    private final boolean _required;

    protected Argument(String name, T defaultValue, boolean required) {
      _name = name;
      _defaultValue = defaultValue;
      _required = false;
    }

    protected Argument(String name, boolean required) {
      this(name,null, required);
    }

    protected void check(Properties args, Properties decoded) throws IllegalArgumentException {
      if (_required && (args.containsKey(_name) == false))
        throw new IllegalArgumentException("Argument "+_name+" must be provided to the request");
      try {
        decoded.put(_name,decode(args.getProperty(_name)));
      } catch (Exception e) {
        throw new IllegalArgumentException("Argument "+_name+" has invalid value: "+e.getMessage());
      }
    }

    protected abstract T decode(String value) throws Exception;

    /** Returns the value of the current argument in the given call.
     *
     */
    public T value(Properties args) {
      if (args.contains(_name))
        return (T) args.get(_name);
      return _defaultValue;
    }

  }

  /** A properly formed key.
   *
   * Checks that the given argument is a properly formed key. Does not check
   * that the key exists in H2O, for this functionality use the
   * ExistingKeyArgument class.
   */
  public class KeyArgument extends Argument<Key> {

    public KeyArgument(String name, Key defaultValue, boolean required) {
      super(name, defaultValue, required);
    }

    @Override protected Key decode(String value) throws Exception {
      Key k = Key.make(value);
      return k;
    }
  }

  /** A properly formed existing key.
   *
   * Returns the value associated with the key itself.
   *
   */
  public class ExistingKeyArgument extends Argument<Value> {

    public ExistingKeyArgument(String name, Value defaultValue, boolean required) {
      super(name, defaultValue, required);
    }

    @Override protected Value decode(String value) throws Exception {
      Value v = DKV.get(Key.make(value));
      if (v == null)
        throw new Exception("key "+value+"not found");
      return v;
    }
  }


  // Dispatch ------------------------------------------------------------------

  public String serve(String requestName, Properties args) {



    return null;
  }

}
