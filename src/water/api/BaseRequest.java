
package water.api;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import water.NanoHTTPD;
import water.web.RString;

/** Base class of all requests that determines how the request handles its
 * arguments. While the code might in theory go to the Request class itself,
 * this is a simple mechanism how to make sure that the code is more modular
 * and more readable.
 *
 * Each request has its own arguments and its serve method can only access the
 * properly parsed arguments.
 *
 * @author peta
 */
public class BaseRequest {

  public static final String JSON_ERROR = "error";

  // ===========================================================================
  // Arguments processing
  // ===========================================================================

  private ThreadLocal<HashMap<String,Object>> _checkedArguments = new ThreadLocal();
  private ThreadLocal<Properties> _originalArguments = new ThreadLocal();

  private ArrayList<Argument> _arguments = new ArrayList();

  // ---------------------------------------------------------------------------

  /** Request Argument accessor & checker.
   *
   * Each request has the serve() method - implemented not here, but in the
   * Request class itself for better clarity. This method has no arguments and
   * the only arguments it can access are those defined by the request itself as
   * its fields. If these inherit from the Argument class they will be
   * automatically checked, defaulted and all errors / queries properly
   * generated. THERE SHOULD BE NO OTHER WAY OF READING USER INPUT ARGUMENTS
   * THAN THROUGH AN APPROPROIATE CHECKER - this is important because otherwise
   * the automatic query generation mechanism does not work properly and all
   * errors raised from serve() method will be reported as HTTP 500 which is
   * not desirable.
   *
   * NOTE While you can in theory just use the default StringArgument class for
   * everything and still do the value checking in the serve method YOU SHOULD
   * NOT!
   *
   * The framework does not distinguish between an empty string and argument
   * not provided by default. If an argument is not provided, the empty string
   * value is used in all functions, where relevant. This is on purpose to make
   * things simpler and to facilitate the HTML form behavior where empty text
   * input might still send its value (empty string).
   *
   * Arguments might depend on each other. For instance a column selector
   * argument does depend on the VAKey argument. While the order of argument
   * creation is preserved while parsing, that is earlier created arguments are
   * parsed as first so that they are available for the later ones, it might
   * happen that the earlier argument failed and has therefore null as its
   * value stored, even if it is required. This must be checked in decode()
   * method.
   *
   * NOTE place all other arguments in here so that they stay in one place.
   */
  public abstract class Argument<T> {

    /** Override this method to determine how the arguent's value in Java is
     * parsed from the text input.
     *
     * Any errors during the parsing should be reported.
     */
    protected abstract T parse(String input) throws Exception;

    /** Description of the value that will be identified as correct argument
     * value. Used as placeholder in the query form.
     */
    public abstract String description();

    /** Returns the default value of the argument. If the argument depends on
     * some other arguments, note that if these fail, their values might be null
     * when calling this method!
     */
    protected T defaultValue() {
      return null;
    }

    /** Determines the HTTP protocol name of the argument. Please use
     * small_letters_with_underscores_instead_of_spaces.
     */
    public final String _name;

    /** Determines whether the argument is required, or if can be omitted by
     * the user.
     */
    public final boolean _required;

    /** Determines the help for the argument (the name of the field in the
     * query HTML form, should be a string in English:)
     */
    private final String _help;

    /** Creates new argument with help being the same as the name of the
     * argument. See the other constructor for more details.
     */
    public Argument(String name, boolean required) {
      this(name, required, name);
    }

    /** Creates the argument of given name and help specification. The argument
     * might be required, or not.
     *
     * Upon creation the argument is added to the list (ordered by the time of
     * creation) of the arguments for the given request automatically.
     */
    public Argument(String name, boolean required, String help) {
      _name = name;
      _required = required;
      _help = help;
      _arguments.add(this);
    }

    /** Help to the argument. The argument name is displayed in the HTML query
     * instead of the HTTP argument name which might be ugly to look at.
     */
    public String help() {
      return _help;
    }

    /** Checks and parses the value of the argument.
     *
     * Checks the argument and attempts to parse it to a proper object. If any
     * error occurs during the parsing, it is reported.
     *
     * The method also stores the input to the originalArguments Properties
     * field of the request object.
     *
     * If the parsing was not successful, or the string empty, the defaultValue
     * method is called and the default value is stored to the checked arguments
     * if it is not null.
     *
     * Summary: After the call to this method, any error during the parsing of
     * the argument is reported. OriginalArguments field for the given argument
     * contains the input string (or empty string if the argument was not
     * specified) and the checkedArguments contains the parsed value, or the
     * default value if parsing was not successful or the string was empty.
     *
     * NOTE that if parsing returns null as the value, then null will indeed
     * appear in the hashmap (not stored, but when value() is called, null will
     * be returned).
     */
    protected void check(String input) throws IllegalArgumentException {
      HashMap<String,Object> checkedArgs = _checkedArguments.get();
      _originalArguments.get().put(_name,input);
      boolean parsed = false;
      try {
        if (input.isEmpty()) {
          if (_required)
            throw new IllegalArgumentException("Argument not specified, but required");
        } else {
          T val = parse(input);
          if (val != null)
            checkedArgs.put(_name, val);
          parsed = true;
        }
      } catch (Exception e) {
        throw new IllegalArgumentException(e.getMessage());
      } finally {
        if (!parsed) {
          T dv = defaultValue();
          if (dv != null)
            checkedArgs.put(_name, dv);
        }
      }
    }

    /** Returns the value of the argument. This method of the argument can be
     * called anytime after the arguments are checked. It simply returns the
     * parsed value, or the default value if the argument is not required and
     * no value was given.
     *
     * NOTE: the default value must be present in the _checkedArguments hashmap
     * if a default value of the argument should be used.
     */
    public final T value() {
      return (T) _checkedArguments.get().get(_name);
    }

    /** Returns true, if the argument was specified in the request. Returns true
     * even if the argument was specified wrongly. Of course this is only valid
     * in queries as you will never get to serve() method if arguments fail
     * their parsing.
     */
    public final boolean specified() {
      return _originalArguments.get().containsKey(_name);
    }

    /** Returns the query HTML code for the argument. The argument's value() is
     * used to determine what to put in the form.
     *
     * By default, each argument displays a simple text field, with its own
     * name, inputValue if any and description used as placeholder.
     *
     * NOTE Override this method if you want to specify a more elaborate input
     * method in the HTML query for the argument.
     */
    protected String query() {
      T value = value();
      return DOM.textInput(_name, value == null ? "" : value.toString(), description());
    }
  }

  // ---------------------------------------------------------------------------

  /** Argument with a simple default value.
   *
   * Any argument that can specify its default value (if any) from its
   * initialization should inherit from this class.
   *
   * @param <T>
   */
  public abstract class DefaultValueArgument<T> extends Argument<T> {

    private T _defaultValue;

    /** Creates an argument with given name, help, and default value. This
     * argument is never required.
     */
    protected DefaultValueArgument(String name, T defaultValue, String help) {
      super(name, false, help);
      _defaultValue = defaultValue;
    }

    /** Creates an argument with given name and help. This argument is always
     * required. Should the defaultValue() method on this argument be called,
     * the result is null.
     *
     * NOTE that null cannot be used to determine whether the argument is
     * required or not, as it may very well be a valid value of the argument
     * itself.
     */
    protected DefaultValueArgument(String name, String help) {
      super(name, true, help);
      _defaultValue = null;
    }

    /** Returns the previously created default value.
     */
    @Override protected T defaultValue() {
      return _defaultValue;
    }
  }

  // ===========================================================================
  // Request dispatch
  // ===========================================================================

  /** Request type.
   *
   * Requests can have multiple types. Basic types include the plain json type
   * in which the result is returned as a JSON object, a html type that acts as
   * the webpage, or the html.help type that displays the extended help for the
   * request.
   *
   * The html.wiki type is also added that displays the markup of the wiki that
   * should be used to document the request as per Matt's suggestion.
   *
   * NOTE the requests are distinguished by their suffixes. Please make the
   * suffix start with the dot character to avoid any problems with request
   * names.
   */
  public static enum RequestType {
    json(""), ///< json type request, a result is a JSON structure
    www(".html"), ///< webpage request
    help(".html.help"), ///< should display the help on the given request
    wiki(".html.wiki") ///< displays the help for the given request in a markup for wiki
    ;
    /** Suffix of the request - extension of the URL.
     */
    public final String _suffix;

    RequestType(String suffix) {
      _suffix = suffix;
    }

    /** Returns the request type of a given URL. JSON request type is the default
     * type when the extension from the URL cannot be determined.
     */
    public static RequestType requestType(String requestUrl) {
      if (requestUrl.endsWith(www._suffix))
        return www;
      if (requestUrl.endsWith(help._suffix))
        return help;
      if (requestUrl.endsWith(wiki._suffix))
        return wiki;
      return json;
    }

    /** Returns the name of the request, that is the request url without the
     * request suffix.
     */
    public String requestName(String requestUrl) {
      return requestUrl.substring(0, requestUrl.length()-_suffix.length());
    }
  }

  // ---------------------------------------------------------------------------

  /** Serves the request returning the response object using the appropriate
   * request type.
   *
   * help and wiki requests are handled completely separately and their
   * arguments are discarded completely. See their respective methods.
   *
   * For json and www requests their argument are first checked. If the
   * arguments are correct, the serve method is called and the response object
   * is returned. In www mode the response object is interpreted by the HTML
   * engine and then returned to the user as web page.
   *
   * In www mode, if some of the arguments are not matched properly, the
   * automatic query build process is initiated and the query is returned
   * instead of the simple error.
   *
   * If in www mode and no arguments are present, the query is provided with no
   * errors.
   */
  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    String result = null; // string to which the result will be stored
    switch (type) {
      case wiki:
        result = serveWiki();
        break;
      case help:
        result = serveHelp();
        break;
      case json:
      case www:
        result = checkArguments(args, type);

    }
  }


  private static final String _queryHtml =
            "<h3>Request %REQ_NAME</h3>"
          + "<p>Please specify the arguments for the request. If you have"
          + " already specified them, but they are wrong, or missing,"
          + " appropriate errors are displayed next to the form inputs.</p>"
          + "<p>Required fields are denoted by a red asterisk"
          + " <span style='color:#ffc0c0'>*</span></p>."
          + "<p><a href='%REQ_NAME.help'>Request help</a></p>"
          + "<form>"
          + "  <div class='control-group'><div class='controls'>"
          + "    <input type='submit' class='btn btn-primary' value='Send request' />"
          + "    <input type='reset' class='btn' value='Clear' />"
          + "  </div></div>"
          + "  %ARG_INPUT_HTML{"
          + "  %ERROR"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='%ARG_NAME'>%ARG_ASTERISK %ARG_HELP</label>"
          + "    <div class='controls'>"
          + "      %ARG_INPUT_CONTROL"
          + "    </div>"
          + "  </div>"
          + "  }"
          + "  <div class='control-group'><div class='controls'>"
          + "    <input type='submit' class='btn btn-primary' value='Send request' />"
          + "    <input type='reset' class='btn' value='Clear' />"
          + "  </div></div>"
          + "</form>"
          ;

  private String checkArguments(Properties args, RequestType type) {
    _originalArguments.set(new Properties());
    _checkedArguments.set(new HashMap());
    for (Argument arg: _arguments) {
      try {
        arg.check(args.getProperty(arg._name,""));
      } catch (IllegalArgumentException e) {
        return jsonError("Argument "+arg._name+" error: "+e.getMessage()).toString();
      }
    }
  }

  protected String buildQuery(Properties args) {
    RString query = new RString(_queryHtml);
    query.replace("REQ_NAME", this.getClass().getSimpleName());
    for (Argument arg: _arguments) {
      RString input = query.restartGroup("ARG_INPUT_HTML");
      input.replace("ARG_NAME",arg._name);
      input.replace("ARG_ASTERISK", DOM.color( arg._required ? "#ff0000" : "#ffffff", "*"));
      input.replace("ARG_HELP", arg.help());
      if (! args.isEmpty()) {
        try {
          arg.check(args.getProperty(arg._name,""));
        } catch (IllegalArgumentException e) {
          input.replace("ERROR","Error: "+e.getMessage());
        }
      }
      input.replace("ARG_INPUT_CONTROL", arg.query());
    }
    return query.toString();
  }


  /** Serves the help of the request page.
   *
   */
  private String serveHelp() {
    return "NOT IMPLEMENTED YET";
  }

  /** Serves the wiki of the request page.
   *
   */
  private String serveWiki() {

    return "NOT IMPLEMENTED YET";
  }



  // ===========================================================================
  // Helpers
  // ===========================================================================

  protected static JsonObject jsonError(String error) {
    JsonObject result = new JsonObject();
    result.addProperty(JSON_ERROR, error);
    return result;
  }


}
