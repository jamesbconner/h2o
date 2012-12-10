
package water.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

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
public class RequestArguments {

  private ThreadLocal<HashMap<String,Object>> _checkedArguments = new ThreadLocal();
  private ThreadLocal<Properties> _originalArguments = new ThreadLocal();

  private ArrayList<Argument> _arguments = new ArrayList();



  // ===========================================================================

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
   *
   * @param <T>
   */
  public abstract class Argument<T> {

    /** Override this method to determine how the arguent's value in Java is
     * parsed from the text input.
     *
     * Any errors during the parsing should be reported.
     *
     * @param input
     * @return
     * @throws Exception
     */
    protected abstract T parse(String input) throws Exception;

    /** Description of the value that will be identified as correct argument
     * value. Used as placeholder in the query form.
     *
     * @return
     */
    public abstract String description();

    /** Help to the argument. The argument name is displayed in the HTML query
     * instead of the HTTP argument name which might be ugly to look at.
     * @return
     */
    public abstract String help();

    /** Returns the default value of the argument. If the argument depends on
     * some other arguments, note that if these fail, their values might be null
     * when calling this method!
     *
     * @return
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
    public final String _help;

    /** Creates new argument with help being the same as the name of the
     * argument. See the other constructor for more details.
     *
     * @param name
     * @param required
     */
    public Argument(String name, boolean required) {
      this(name, required, name);
    }

    /** Creates the argument of given name and help specification. The argument
     * might be required, or not.
     *
     * Upon creation the argument is added to the list (ordered by the time of
     * creation) of the arguments for the given request automatically.
     *
     * @param name
     * @param required
     * @param help
     */
    public Argument(String name, boolean required, String help) {
      _name = name;
      _required = required;
      _help = help;
      _arguments.add(this);
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
     *
     * @param input
     * @throws IllegalArgumentException
     */
    protected void check(String input) throws IllegalArgumentException {
      HashMap<String,Object> checkedArgs = _checkedArguments.get();
      _originalArguments.get().put(_name,input);
      boolean parsed = false;
      try {
        if (input.isEmpty()) {
          if (_required)
          throw new IllegalArgumentException("Argument "+_name+" not specified, but required");
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
     *
     * @return
     */
    public final T value() {
      return (T) _checkedArguments.get().get(_name);
    }

    /** Returns the query HTML code for the argument, given the inputValue
     * was supplied by the user. If the user did not submit any value, an empty
     * string is passed to this method as a value.
     *
     * By default, each argument displays a simple text field, with its own
     * name, inputValue if any and description used as placeholder.
     *
     * NOTE Override this method if you want to specify a more elaborate input
     * method in the HTML query for the argument.
     *
     * @param inputValue
     * @return
     */
    protected String query(String inputValue) {
      return DOM.textInput(_name, inputValue, description());
    }
  }

}
