
package water.api;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import init.Boot;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import water.*;
import water.web.Page;
import water.web.RString;
import water.web.Server;

/** A basic class for a JSON request.
 *
 *
 *
 * @author peta
 */

public abstract class Request {

  public static final String JSON_ERROR = "Error";
  public static final String JSON_ERROR_TYPE = "ErrorType";



  // override those to get the functionality required --------------------------

  /** Serves the given request and returns the resulting JSON object.
   *
   * @param args
   * @return
   */
  public abstract void serve(JsonObject response,Properties args);


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

  // basic API for creating requests -------------------------------------------


  protected String _href;

  public String href() {
    return _href;
  }

  protected void setHref(String href) {
    assert (_href == null) : "href can only be set once - setting from "+_href+" to "+href;
    _href = href;
  }

  // argument checking ---------------------------------------------------------

  private ArrayList<Argument> _arguments = new ArrayList();


  /** An argument to the request.
   *
   * The argument (its subclasses) provide a checked version of arguments of
   * different types being parsed to the request. Each request must register
   * its arguments beforehand in a static constructor.
   *
   * @param <T>
   */
  public abstract class Argument<T> {

    protected final String _name;
    protected final boolean _required;

    protected Argument(String name, boolean required) {
      _name = name;
      _required = false;
      _arguments.add(this);
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

    public abstract T defaultValue();

    /** Returns the value of the current argument in the given call.
     *
     */
    public T value(Properties args) {
      if (args.contains(_name))
        return (T) args.get(_name);
      return defaultValue();
    }
  }

  /** An argument with simple default value stored in it as a variable.
   *
   * Inherit from this guy if your default values are simple and can easily be
   * computed when the argument is created.
   *
   * @param <T>
   */
  public abstract class DefaultValueArgument<T> extends Argument<T> {
    private final T _defaultValue;

    protected DefaultValueArgument(String name) {
      super(name, true);
      _defaultValue = null;
    }

    protected DefaultValueArgument(String name, T defaultValue) {
      super(name, false);
      _defaultValue = defaultValue;
    }

    public T defaultValue() {
      assert (_required == false) : "You never ask defaultValue of a required argument";
      return _defaultValue;
    }

  }

  /** A simple String argument checker. Does not really do anything because there
   * is not much to check in a string argument.
   */
  public class StringArgument extends DefaultValueArgument<String> {

    public StringArgument(String name, String defaultValue) {
      super(name, defaultValue);
    }

    public StringArgument(String name) {
      super(name);
    }

    @Override protected String decode(String value) throws Exception {
      return value;
    }

  }

  /** A properly formed key.
   *
   * Checks that the given argument is a properly formed key. Does not check
   * that the key exists in H2O, for this functionality use the
   * ExistingKeyArgument class.
   */
  public class KeyArgument extends DefaultValueArgument<Key> {

    public KeyArgument(String name, Key defaultValue) {
      super(name, defaultValue);
    }

    public KeyArgument(String name) {
      super(name);
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
  public class ExistingKeyArgument extends DefaultValueArgument<Value> {

    public ExistingKeyArgument(String name, Value defaultValue) {
      super(name, defaultValue);
    }

    public ExistingKeyArgument(String name) {
      super(name);
    }

    @Override protected Value decode(String value) throws Exception {
      Value v = DKV.get(Key.make(value));
      if (v == null)
        throw new Exception("key "+value+"not found");
      return v;
    }
  }

  // Dispatch ------------------------------------------------------------------

  /** Dispatches the request.
   *
   * The request dispatch checks the arguments first, then calls the serve to
   * the JSON object and returns the result if in JSON mode. If in HTML mode,
   * the JSON response is then processed by the createHTML() method and its
   * result returned.
   *
   * Any errors are reported accordingly.
   *
   * If there are arguments missing, or no arguments at all, they are reported
   * and if createQuery() method is working, the query for the request is
   * presented if in HTML mode.
   *
   * @param args
   * @param isHTML
   * @return
   */
  public NanoHTTPD.Response dispatch(NanoHTTPD server, Properties args, boolean isHTML) {
    // create the JSON object for the serve method
    JsonObject response = new JsonObject();
    try {
      // check all arguments and create the parsed arguments hashtable
      Properties parsedArgs = checkArguments(args);
      // server the request
      serve(response,parsedArgs);
      // if we are in HTML, create the response string and return it
      if (isHTML)
        return wrap(server,createHtml(response));
    } catch (IllegalArgumentException e) {
      // if we have illegal arguments, get the queryHTML page and display the
      // error if we have at least some arguments specified
      if (isHTML) {
        StringBuilder sb = new StringBuilder();
        String query = createQuery(args);
        // if empty arguments, or no query, display the error
        if (!args.isEmpty() || query == null)
          DOM.error(sb,e.getMessage());
        // append the query form if created
        if (query != null)
          sb.append(query);
        // return the contents
        return wrap(server,sb.toString());
      } else {
        // for JSON just store the error in the response
        response.addProperty(JSON_ERROR,e.getMessage());
      }
    } catch (Exception e) {
      // anything else, store to response's error field
      response = new JsonObject();
      response.addProperty(JSON_ERROR,e.getMessage());
    }
    // at the end return the JSON, assuming that JSON is what we want
    return wrap(server,response);
  }

  private NanoHTTPD.Response wrap(NanoHTTPD server, String response) {
    RString html = new RString(htmlTemplate);
    html.replace("CONTENTS",response);
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_HTML, html.toString());
  }

  private NanoHTTPD.Response wrap(NanoHTTPD server, JsonObject response) {
    return server.new Response(NanoHTTPD.HTTP_OK, NanoHTTPD.MIME_JSON, response.toString());
  }

  private Properties checkArguments(Properties args) throws IllegalArgumentException {
    Properties result = new Properties();
    for (Argument arg : _arguments)
      arg.check(args,result);
    return result;
  }



  //

  private static String htmlTemplate;

  // HTML template used for all pages

  static {
    InputStream resource = Boot._init.getResource2("/page.html");
    try {
      htmlTemplate = new String(ByteStreams.toByteArray(resource));
    } catch (NullPointerException e) {
      Log.die("page.html not found in resources.");
    } catch (Exception e) {
      Log.die(e.getMessage());
    } finally {
      Closeables.closeQuietly(resource);
    }
  }





}
