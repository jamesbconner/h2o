
package water.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import water.*;

/**
 *
 * @author peta
 */
public class RequestArguments extends RequestStatics {

  protected ThreadLocal<HashMap<String,Object>> _checkedArguments = new ThreadLocal();
  protected ThreadLocal<Properties> _originalArguments = new ThreadLocal();

  protected ArrayList<Argument> _arguments = new ArrayList();

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
  // Any defined Argument type should go here:
  // ===========================================================================

  /** Any string argument.
   *
   * Performs *no* checks at all so accepts any string.
   *
   * NOTE that unless default value empty string is defined, the string will
   * not accept an empty string.
   */
  public class Str extends DefaultValueArgument<String> {

    /** Creates a required string argument. Does not accept even empty string.
     */
    public Str(String name, String help) {
      super(name, help);
    }

    /** Creates an optional string argument. Default value must be specified.
     */
    public Str(String name, String defaultValue, String help) {
      super(name, defaultValue, help);
    }

    /** Parses the string. The string is simple returned.
     */
    @Override protected String parse(String input) throws Exception {
      return input;
    }

    /** Any string will do, non-empty if required.
     */
    @Override public String description() {
      return _required ? "any nonempty string" : "any string";
    }
  }

  // ---------------------------------------------------------------------------

  /** Integer argument.
   *
   */

  public class Int extends DefaultValueArgument<Integer> {

    public final int _min;
    public final int _max;

    public Int(String name, String help) {
      this(name,help, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public Int(String name, String help, int min, int max) {
      super(name, help);
      _min = min;
      _max = max;
    }

    public Int(String name, int defaultValue, String help) {
      this(name, defaultValue, help, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public Int(String name, int defaultValue, String help, int min, int max) {
      super(name, defaultValue, help);
      _min = min;
      _max = max;
    }

    @Override protected Integer parse(String input) throws Exception {
      int result;
      try {
        result = Integer.parseInt(input);
      } catch (NumberFormatException e) {
        throw new Exception(input+" is not a valid integer");
      }
      if ((result<_min) || (result>_max))
        throw new Exception(input+" is not from "+_min+" to "+_max+" (inclusive)");
      return result;
    }

    @Override public String description() {
      if ((_min == Integer.MIN_VALUE) && (_max == Integer.MAX_VALUE))
        return "any integer number";
      return "integer from "+_min+" to "+_max+" (inclusive)";
    }
  }

  // ---------------------------------------------------------------------------

  /** Real argument
   *
   */
  public class Real extends DefaultValueArgument<Double> {

    public final Double _min;
    public final Double _max;

    public Real(String name, String help) {
      this(name, help, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public Real(String name, String help, double min, double max) {
      super(name, help);
      _min = min;
      _max = max;
    }

    public Real(String name, double defaultValue, String help) {
      this(name, defaultValue, help, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public Real(String name, double defaultValue, String help, double min, double max) {
      super(name, defaultValue, help);
      _min = min;
      _max = max;
    }

    @Override
    protected Double parse(String input) throws Exception {
      double result;
      try {
        result = Double.parseDouble(input);
      } catch (NumberFormatException e) {
        throw new Exception(input+" is not a valid real number");
      }
      if ((result<_min) || (result>_max))
        throw new Exception(input+" is not from "+_min+" to "+_max+" (inclusive)");
      return result;
    }

    @Override
    public String description() {
      if ((_min == Double.NEGATIVE_INFINITY) && (_max == Double.POSITIVE_INFINITY))
        return "any real number";
      return "real number from "+_min+" to "+_max+" (inclusive)";
    }
  }

  // ---------------------------------------------------------------------------

  /** Boolean argument.
   *
   */
  public class Bool extends DefaultValueArgument<Boolean> {

    // is always optional, default is always false
    public Bool(String name, String help) {
      super(name, false, help);
    }

    @Override protected Boolean parse(String input) throws Exception {
      if (input.equals("1"))
        return true;
      if (input.equals("0"))
        return false;
      throw new Exception("value "+input+" is not boolean (1 or 0 accepted only)");
    }

    @Override public String description() {
      return super.help();
    }

    @Override public String help() {
      return "";
    }

    @Override protected String query() {
      return DOM.checkbox(_name, value(), description());
    }
  }

  // ---------------------------------------------------------------------------

  public class H2OKey extends DefaultValueArgument<Key> {

    public H2OKey(String name, String help) {
      super(name, help);
    }

    public H2OKey(String name, String keyName, String help) {
      super(name, Key.make(keyName), help);
    }

    public H2OKey(String name, Key key, String help) {
      super(name, key, help);
    }

    @Override protected Key parse(String input) throws Exception {
      return Key.make(input);
    }

    @Override public String description() {
      // TODO what actually is a valid key name? I know I should now, but it is
      // better to have tests for it:)
      return "a valid H2O key name";
    }
  }

  // ---------------------------------------------------------------------------

  public class H2OExistingKey extends Argument<Value> {

    // IS ALWAYS REQUIRED
    public H2OExistingKey(String name, String help) {
      super(name, true, help);
    }

    @Override protected Value parse(String input) throws Exception {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)
        throw new Exception("key "+input+" does not exist");
      return v;
    }

    @Override public String description() {
      return "an existing H2O key";
    }
  }

  // ---------------------------------------------------------------------------

  public class H2OValueArrayKey extends Argument<ValueArray> {

    // IS ALWAYS REQUIRED
    public H2OValueArrayKey(String name, String help) {
      super(name, true, help);
    }

    @Override protected ValueArray parse(String input) throws Exception {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)
        throw new Exception("key "+input+" does not exist!");
      if (!(v instanceof ValueArray))
        throw new Exception("key "+input+" does not point to a HEX file");
      return (ValueArray)v;
    }

    @Override public String description() {
      // TODO how do we call these keys for customers?
      return "an existing hex key";
    }
  }

  // ---------------------------------------------------------------------------




}
