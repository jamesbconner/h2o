
package water.api;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import init.Boot;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
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

  public static final String JSON_ERROR = "error";
  public static final String JSON_ERROR_TYPE = "error_type";



  // override those to get the functionality required --------------------------

  /** Serves the given request and returns the resulting JSON object.
   *
   * @param args
   * @return
   */
  public abstract void serve(JsonObject response);


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
  protected void createHTMLBuilders(HTMLBuilder builder) {
    // pass
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

  private ThreadLocal<Properties> _objectArguments = new ThreadLocal();



  /** An argument to the request.
   *
   * The argument (its subclasses) provide a checked version of arguments of
   * different types being parsed to the request. Each request must register
   * its arguments beforehand in a static constructor.
   *
   * @param <T>
   */
  public abstract class Argument<T> {

    public final String _name;
    public final boolean _required;
    public final String _help;

    protected Argument(String name, boolean required, String help) {
      _name = name;
      _required = required;
      _arguments.add(this);
      _help = help;
    }

    protected T check(Properties args) throws IllegalArgumentException {
      if (args.containsKey(_name) == false) {
        if (_required)
          throw new IllegalArgumentException("Argument "+_name+" must be provided to the request");
        else
          return null;
      }
      try {
        return decode(args.getProperty(_name));
      } catch (Exception e) {
        throw new IllegalArgumentException("Argument "+_name+" has invalid value: "+e.getMessage());
      }
    }

    protected abstract T decode(String value) throws Exception;

    public abstract T defaultValue();

    /** Returns the value of the current argument in the given call.
     *
     */
    public T value() {
      Properties args = _objectArguments.get();
      if (args.containsKey(_name))
        return (T) args.get(_name);
      return defaultValue();
    }

    public boolean specified() {
      Properties args = _objectArguments.get();
      return args.containsKey(_name);
    }

    /** Adds the query part for the query that is being built. Value is the
     * original value submitted by the request, if any. If no value submitted,
     * then value is null. Null is returned also if the argument was empty.
     *
     * @param sb
     * @param value
     */
    public String buildQuery(String value) {
      if (value == null) {
        T v = defaultValue();
        if (v!=null)
          value = v.toString();
      }
      return DOM.textInput(_name, value, description());
    }

    /** Returns the description of the values of the argument. This is not what
     * the argument means, but what are the available values. Null by default.
     * @return
     */
    public String description() {
      return "";
    };

    public String help() {
      return _help;
    }

    public final String requiredHTML() {
      if (_required)
        return DOM.color("*","#ff0000");
      else
        return DOM.color("*","#ffffff");
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

    protected DefaultValueArgument(String name, String help) {
      super(name, true, help);
      _defaultValue = null;
    }

    protected DefaultValueArgument(String name, T defaultValue, String help) {
      super(name, false, help);
      _defaultValue = defaultValue;
    }

    public T defaultValue() {
      return _defaultValue;
    }

  }

  /** A simple String argument checker. Does not really do anything because there
   * is not much to check in a string argument.
   */
  public class StringArgument extends DefaultValueArgument<String> {

    public StringArgument(String name, String defaultValue, String help) {
      super(name, defaultValue, help);
    }

    public StringArgument(String name, String help) {
      super(name, help);
    }

    @Override protected String decode(String value) throws Exception {
      return value;
    }

    @Override public String description() {
      return "Any string value.";
    }

  }

  /** A properly formed key.
   *
   * Checks that the given argument is a properly formed key. Does not check
   * that the key exists in H2O, for this functionality use the
   * ExistingKeyArgument class.
   */
  public class KeyArgument extends DefaultValueArgument<Key> {

    public KeyArgument(String name, Key defaultValue, String help) {
      super(name, defaultValue, help);
    }

    public KeyArgument(String name, String help) {
      super(name, help);
    }

    @Override protected Key decode(String value) throws Exception {
      if (value.isEmpty())
        throw new Exception("Key cannot be empty");
      Key k = Key.make(value);
      return k;
    }

    @Override public String description() {
      return "Valid key name (a-z,A-Z,0-9, space, ...).";
    }

  }

  /** A properly formed existing key.
   *
   * Returns the value associated with the key itself.
   *
   */
  public class ExistingKeyArgument extends DefaultValueArgument<Key> {

    public ExistingKeyArgument(String name, Key defaultValue, String help) {
      super(name, defaultValue, help);
    }

    public ExistingKeyArgument(String name, String help) {
      super(name, help);
    }

    @Override protected Key decode(String value) throws Exception {
      if (value.isEmpty())
        throw new Exception("Key cannot be empty");
      Key k = Key.make(value);
      Value v = DKV.get(Key.make(value));
      if (v == null)
        throw new Exception("key "+value+"not found");
      return k;
    }

    @Override public String description() {
      return "Key that already exists in H2O.";
    }
  }

  public class IntegerArgument extends DefaultValueArgument<Integer> {

    public final int _min;
    public final int _max;

    public IntegerArgument(String name, int defaultValue, String help) {
      super(name, defaultValue, help);
      _min = Integer.MIN_VALUE;
      _max = Integer.MAX_VALUE;
    }

    public IntegerArgument(String name, int min, int max, Integer defaultValue, String help) {
      super(name, defaultValue, help);
      _min = min;
      _max = max;
    }

    @Override protected Integer decode(String value) throws Exception {
      try {
        int i = Integer.valueOf(value);
        if ((i<_min) || (i >=_max))
          throw new Exception("value "+i+" out of allowed range <"+_min+" ; "+_max+")");
        return i;
      } catch (NumberFormatException e) {
        throw new Exception("not a valid number format: "+value);
      }
    }

    @Override public String description() {
      return "Integer value in range <"+_min+", "+_max+").";
    }
  }

  public class BooleanArgument extends DefaultValueArgument<Boolean> {

    public BooleanArgument(String name, String help) {
      super(name, false, help); // bollean alrgument cannot really be required. Its omission is false
    }

    public BooleanArgument(String name, boolean defaultValue, String help) {
      super(name, defaultValue, help);
    }

    @Override protected Boolean decode(String value) throws Exception {
      if (value.equals("1"))
        return true;
      if (value.equals("0"))
        return false;
      if (value.isEmpty())
        return false;
      throw new Exception("Invalid value "+value+" for boolean - only 1 or 0 can be used");
    }

    @Override public String buildQuery(String value) {
      if (value == null)
        value = defaultValue() ? "1" : "0";
      return DOM.checkbox(_name, (value!=null) && value.equals("1"), description());
    }

    @Override public String description() {
      return super.help();
    }

    @Override public String help() {
      return "";
    }

  }

  // Query building ------------------------------------------------------------

  private static final String _formInput =
            "%ERROR"
          + "<div class='control-group'>"
          + "  <label class='control-label' for='%ARG_NAME'>%ARG_HELP:%ARG_ASTERISK</label>"
          + "  <div class='controls'>"
          + "    %ARG_INPUT"
          + "  </div>"
          + "</div>"
          ;

  /** Creates the HTML query page for the request, if any.
   *
   * @param submittedArgs
   * @return
   */
  protected String createQuery(Properties submittedArgs) {
    StringBuilder sb = new StringBuilder();
    sb.append(DOM.h3(_href+" request"));
    boolean reportErrors = ! submittedArgs.isEmpty();
    sb.append(DOM.p("Please fill in the following arguments to the request. When you are done, press the <i>Send Request</i> button."));
    if (reportErrors)
      sb.append(DOM.p("Invalid or missing values are highlighted by the errors found for your convenience. Please correct them and then resend the request."));
    sb.append(DOM.p("Required values are marked with a red asterisk - "+DOM.color("*","#ff0000")+"."));
    sb.append("<form class='form-horizontal'>");
    for (Argument arg : _arguments) {
      RString input = new RString(_formInput);
      String error = processQueryArguments(submittedArgs, arg);
      if (error != null && reportErrors)
        input.replace("ERROR", error);
      input.replace("ARG_NAME", arg._name);
      input.replace("ARG_HELP", arg.help());
      input.replace("ARG_ASTERISK", arg.requiredHTML());
      input.replace("ARG_INPUT", arg.buildQuery(submittedArgs.getProperty(arg._name)));
      sb.append(input.toString());
    }
    sb.append("<div class='controls'>");
    sb.append("  <input type='submit' class='btn btn-primary' value='Send request' />");
    sb.append("  <input type='reset' class='btn' value='Clear' />");
    sb.append("</div>");
    sb.append("</form>");
    return sb.toString();
  }

  /** Processes the argument for the query. If the argument is given, it is
   * checked and its toString value used. If the value is not specified, either
   * its default value is used, or the empty string is specified if it defaults
   * to null.
   *
   * Returns the error to be reported to user, if any.
   *
   * @param submittedArgs
   * @param arg
   * @param reportErrors
   * @return
   */
  private String processQueryArguments(Properties submittedArgs, Argument arg) {
    //Object o = null;
    String error = null;
    try {
      //o =
      arg.check(submittedArgs);
    } catch (IllegalArgumentException e) {
      error = DOM.error(e.getMessage());
    }
    if (submittedArgs.getProperty(arg._name,"").equals(""))
      submittedArgs.remove(arg._name);
    //if (o == null && !arg._required)
    //  o = arg.defaultValue();
    //submittedArgs.put(arg._name, nullToEmptyString(o).toString());
    return error;
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
      _objectArguments.set(checkArguments(args));
      // server the request
      serve(response);
      // if we are in HTML, create the response string and return it
      if (isHTML) {
        HTMLBuilder builder = new HTMLBuilder(this, response);
        createHTMLBuilders(builder);
        return wrap(server, builder.build());
      }
    } catch (IllegalArgumentException e) {
      // if we have illegal arguments, get the queryHTML page and display the
      // error if we have at least some arguments specified
      if (isHTML) {
        StringBuilder sb = new StringBuilder();
        String query = createQuery(args);
        // if empty arguments, or no query, display the error
        if ( query.isEmpty())
          sb.append(DOM.error(e.getMessage()));
        // append the query form if created
        else
          sb.append(query);
        // return the contents
        return wrap(server,sb.toString());
      } else {
        // for JSON just store the error in the response
        response.addProperty(JSON_ERROR,e.getMessage());
      }
    } catch (Exception e) {
      e.printStackTrace();
      // at the end return the JSON, assuming that JSON is what we want
      if (isHTML)
        return wrap(server, DOM.error(e.getMessage()));
      // anything else, store to response's error field
      response = new JsonObject();
      response.addProperty(JSON_ERROR,e.getMessage());
    } finally {
      _objectArguments.set(null);
    }
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
    for (Argument arg : _arguments) {
      Object o = arg.check(args);
      if (o != null)
        result.put(arg._name, o);
    }
    return result;
  }

  // html template and navbar handling -----------------------------------------

  private static String htmlTemplate;

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

  private static class MenuItem {
    public final Request _request;
    public final String _name;
    public MenuItem(Request request, String name) {
      _request = request;
      _name = name;
    }
    public void toHTML(StringBuilder sb) {
      sb.append("<li><a href='");
      sb.append(DOM.urlEncode(_request._href+".html"));
      sb.append("'>");
      sb.append(_name);
      sb.append("</a></li>");
    }

  }

  private static HashMap<String, ArrayList<MenuItem> > _navbar = new HashMap();
  private static ArrayList<String> _navbarOrdering = new ArrayList();



  public static void initializeNavBar() {
    StringBuilder sb = new StringBuilder();
    for (String s : _navbarOrdering) {
      ArrayList<MenuItem> arl = _navbar.get(s);
      if ((arl.size() == 1) && arl.get(0)._name.equals(s)) {
        arl.get(0).toHTML(sb);
      } else {
        sb.append("<li class='dropdown'>");
        sb.append("<a href='#' class='dropdown-togg;e' data-toggle='dropdown'>");
        sb.append(s);
        sb.append("<b class='caret'></b>");
        sb.append("</a>");
        sb.append("<ul class='dropdown-menu'>");
        for (MenuItem i : arl)
          i.toHTML(sb);
        sb.append("</ul></li>");
      }
    }
    RString str = new RString(htmlTemplate);
    str.replace("NAVBAR",sb.toString());
    str.replace("CONTENTS","%CONTENTS");
    htmlTemplate = str.toString();
  }

  public static Request addToNavbar(Request r, String name) {
    assert (! _navbar.containsKey(name));
    ArrayList<MenuItem> arl = new ArrayList();
    arl.add(new MenuItem(r,name));
    _navbar.put(name,arl);
    _navbarOrdering.add(name);
    return r;
  }

  public static Request addToNavbar(Request r, String name, String category) {
    ArrayList<MenuItem> arl = _navbar.get(category);
    if (arl == null) {
      arl = new ArrayList();
      _navbar.put(category,arl);
      _navbarOrdering.add(category);
    }
    arl.add(new MenuItem(r,name));
    return r;
  }





}
