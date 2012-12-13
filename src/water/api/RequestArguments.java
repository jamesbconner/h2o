
package water.api;

import java.util.*;
import water.*;
import water.web.RString;

/**
 *
 * @author peta
 */
public class RequestArguments extends RequestStatics {

  protected ArrayList<Argument> _arguments = new ArrayList();

  public static abstract class Argument<T> {

    protected abstract T parse(String input) throws IllegalArgumentException;

    protected abstract T defaultValue();

    protected abstract String query();

    protected abstract String refreshJS(String callbackName);

    protected abstract String valueJS();

    protected abstract String queryDescription();

    protected String queryAddons() {
      return "";
    }

    protected String queryName() {
      return _name;
    }

    protected static class Record {
      public String _originalValue = null;
      public Object _parsedValue = null;
      public String _disabledReason = null;
      public boolean _valid = false;

      public boolean disabled() {
        return _disabledReason != null;
      }

      public boolean valid() {
        return _valid;
      }

      public boolean specified() {
        return valid() && _originalValue != null;
      }
    }


    public String _requestHelp;
    public final String _name;
    public final boolean _required;
    public final boolean _refreshOnChange;
    public ArrayList<Argument> _dependencies = null;

    private ThreadLocal<Record> _argumentRecord;


    protected Argument(String name, boolean required, boolean refreshOnChange) {
      _name = name;
      _required = required;
      _refreshOnChange = refreshOnChange;
      assert(false);
      //_arguments.add(this);
    }


    protected final void addDependency(Argument arg) {
      if (_dependencies == null)
        _dependencies = new ArrayList();
      _dependencies.add(arg);
    }

    protected final Record record() {
      return _argumentRecord.get();
    }

    public final void disable(String reason) {
      record()._disabledReason = reason;
    }

    public final boolean disabled() {
      return record().disabled();
    }

    public final boolean valid() {
      return record().valid();
    }

    public final boolean specified() {
      return record().specified();
    }

    public final T value() {
      return (T) record()._parsedValue;
    }

    public final void reset() {
      _argumentRecord.set(new Record());
    }

    public void check(String input) throws IllegalArgumentException {
      // get the record -- we assume we have been reset properly
      Record record = record();
      // check that the input is canonical == value or null and store it to the
      // record
      if (input.isEmpty())
        input = null;
      record._originalValue = input;
      // there is not much to do if we are disabled
      if (record.disabled())
        return;
      // check that we have all prerequisities properly initialized
      if (_dependencies != null) {
        for (Argument dep : _dependencies)
          if (!dep.valid()) {
            record._disabledReason = "Not all prerequisite arguments have been supplied: "+dep._name;
            return;
          }
      }
      // if input is null, throw if required, otherwise use the default value
      if (input == null) {
        if (_required)
          throw new IllegalArgumentException("Argument "+_name+" is required, but not specified");
        record._parsedValue = defaultValue();
        record._valid = true;
      // parse the argument, if parse throws we will still be invalid correctly
      } else {
        record._parsedValue = parse(input);
        record._valid = true;
      }
    }
  }

  // ===========================================================================
  // InputText
  // ===========================================================================

  public static abstract class InputText<T> extends Argument<T> {

    public InputText(String name, boolean required, boolean refreshOnChange) {
      super(name, required, refreshOnChange);
    }

    @Override protected String query() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, try the one provided by the
      // default value
      if (value == null) {
        T v = defaultValue();
        value = (v == null) ? "" : v.toString();
      }
      return "<input class='span5' type='text' name='"+_name+"' id='"+_name+"' placeholder='"+queryDescription()+"' "+ (!value.isEmpty() ? (" value='"+value+"' />") : "/>");
    }

    @Override protected String refreshJS(String callbackName) {
      return "$('#"+_name+"').change(callback);";
    }

    @Override protected String valueJS() {
      return "return $('#"+_name+"').val();";
    }
  }

  // ===========================================================================
  // InputCheckBox
  // ===========================================================================


  public static class InputCheckBox extends Argument<Boolean> {

    public final boolean _defaultValue;

    public InputCheckBox(String name, boolean refreshOnChange, boolean defaultValue) {
      super(name, false, refreshOnChange); // checkbox is never required
      _defaultValue = defaultValue;
    }

    @Override protected String query() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, use the provided one
      if (value == null) {
        Boolean v = defaultValue();
        value = ((v == null) || (v == false)) ? "" : "1" ;
      }
      return "<input class='span5' type='checkbox' name='"+_name+"' id='"+_name+"' "+ (value.isEmpty() ? (" checked />") : "/>");
    }

    @Override protected String refreshJS(String callbackName) {
      return "$('#"+_name+"').change(callback);";
    }

    @Override protected String valueJS() {
      return "return $('#"+_name+"').val();";
    }

    @Override protected Boolean parse(String input) throws IllegalArgumentException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override protected Boolean defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }




//  protected ThreadLocal<HashMap<String,Object>> _checkedArguments = new ThreadLocal();
//  protected ThreadLocal<Properties> _originalArguments = new ThreadLocal();
//  protected ThreadLocal<Properties> _disabled = new ThreadLocal();


//  // ---------------------------------------------------------------------------
//
//  /** Request Argument accessor & checker.
//   *
//   * Each request has the serve() method - implemented not here, but in the
//   * Request class itself for better clarity. This method has no arguments and
//   * the only arguments it can access are those defined by the request itself as
//   * its fields. If these inherit from the Argument class they will be
//   * automatically checked, defaulted and all errors / queries properly
//   * generated. THERE SHOULD BE NO OTHER WAY OF READING USER INPUT ARGUMENTS
//   * THAN THROUGH AN APPROPROIATE CHECKER - this is important because otherwise
//   * the automatic query generation mechanism does not work properly and all
//   * errors raised from serve() method will be reported as HTTP 500 which is
//   * not desirable.
//   *
//   * NOTE While you can in theory just use the default StringArgument class for
//   * everything and still do the value checking in the serve method YOU SHOULD
//   * NOT!
//   *
//   * The framework does not distinguish between an empty string and argument
//   * not provided by default. If an argument is not provided, the empty string
//   * value is used in all functions, where relevant. This is on purpose to make
//   * things simpler and to facilitate the HTML form behavior where empty text
//   * input might still send its value (empty string).
//   *
//   * Arguments might depend on each other. For instance a column selector
//   * argument does depend on the VAKey argument. While the order of argument
//   * creation is preserved while parsing, that is earlier created arguments are
//   * parsed as first so that they are available for the later ones, it might
//   * happen that the earlier argument failed and has therefore null as its
//   * value stored, even if it is required. This must be checked in decode()
//   * method.
//   *
//   * NOTE place all other arguments in here so that they stay in one place.
//   */
//  public abstract class Argument<T> {
//
//    /** Fill in this string to have it displayed at the help page generated
//     * automatically by the framework.
//     */
//    public String _requestHelp = null;
//
//    /** Override this method to determine how the arguent's value in Java is
//     * parsed from the text input.
//     *
//     * Any errors during the parsing should be reported.
//     */
//    protected abstract T parse(String input) throws Exception;
//
//    /** Description of the value that will be identified as correct argument
//     * value. Used as placeholder in the query form.
//     */
//    public abstract String description();
//
//    /** Returns the default value of the argument. If the argument depends on
//     * some other arguments, note that if these fail, their values might be null
//     * when calling this method!
//     */
//    protected T defaultValue() {
//      return null;
//    }
//
//    /** Determines the HTTP protocol name of the argument. Please use
//     * small_letters_with_underscores_instead_of_spaces.
//     */
//    public final String _name;
//
//    /** Determines whether the argument is required, or if can be omitted by
//     * the user.
//     */
//    public final boolean _required;
//
//    /** Determines the help for the argument (the name of the field in the
//     * query HTML form, should be a string in English:)
//     */
//    private final String _help;
//
//    /** Creates new argument with help being the same as the name of the
//     * argument. See the other constructor for more details.
//     */
//    public Argument(String name, boolean required) {
//      this(name, required, name);
//    }
//
//    /** Creates the argument of given name and help specification. The argument
//     * might be required, or not.
//     *
//     * Upon creation the argument is added to the list (ordered by the time of
//     * creation) of the arguments for the given request automatically.
//     */
//    public Argument(String name, boolean required, String help) {
//      _name = name;
//      _required = required;
//      _help = help;
//      _arguments.add(this);
//    }
//
//    /** Help to the argument. The argument name is displayed in the HTML query
//     * instead of the HTTP argument name which might be ugly to look at.
//     */
//    public String help() {
//      return _help;
//    }
//
//    /** Creates the request help page part for the given argument. Displays its
//     * JSON name, query name (the one in HTML), value type and the request help
//     * provided by the argument.
//     */
//    public void requestHelp(StringBuilder sb) {
//      sb.append(DOM.h4(_name+DOM.color("*", _required ? "#ff0000" : "#ffffff")));
//      sb.append(DOM.dlHorizontal("JSON name",_name));
//      sb.append(DOM.dlHorizontal("Query name",_help));
//      sb.append(DOM.dlHorizontal("Value type",description()));
//      if (_requestHelp != null)
//        sb.append(DOM.dlHorizontal(" ",_requestHelp));
//    }
//
//    /** Checks and parses the value of the argument.
//     *
//     * Checks the argument and attempts to parse it to a proper object. If any
//     * error occurs during the parsing, it is reported.
//     *
//     * The method also stores the input to the originalArguments Properties
//     * field of the request object.
//     *
//     * If the parsing was not successful, or the string empty, the defaultValue
//     * method is called and the default value is stored to the checked arguments
//     * if it is not null.
//     *
//     * Summary: After the call to this method, any error during the parsing of
//     * the argument is reported. OriginalArguments field for the given argument
//     * contains the input string (or empty string if the argument was not
//     * specified) and the checkedArguments contains the parsed value, or the
//     * default value if parsing was not successful or the string was empty.
//     *
//     * NOTE that if parsing returns null as the value, then null will indeed
//     * appear in the hashmap (not stored, but when value() is called, null will
//     * be returned).
//     */
//    protected void check(String input) throws IllegalArgumentException {
//      HashMap<String,Object> checkedArgs = _checkedArguments.get();
//      _originalArguments.get().put(_name,input);
//      boolean parsed = false;
//      try {
//        if (input.isEmpty()) {
//          if (_required)
//            throw new IllegalArgumentException("Argument not specified, but required");
//        } else {
//          T val = parse(input);
//          if (val != null)
//            checkedArgs.put(_name, val);
//          parsed = true;
//        }
//      } catch (Exception e) {
//        //e.printStackTrace();
//        throw new IllegalArgumentException(e.getMessage());
//      } finally {
//        if (!parsed) {
//          T dv = defaultValue();
//          if (dv != null)
//            checkedArgs.put(_name, dv);
//        }
//      }
//    }
//
//    /** Returns the value of the argument. This method of the argument can be
//     * called anytime after the arguments are checked. It simply returns the
//     * parsed value, or the default value if the argument is not required and
//     * no value was given.
//     *
//     * NOTE: the default value must be present in the _checkedArguments hashmap
//     * if a default value of the argument should be used.
//     */
//    public final T value() {
//      return (T) _checkedArguments.get().get(_name);
//    }
//
//    /** Returns true, if the argument was specified in the request. Returns true
//     * even if the argument was specified wrongly. Of course this is only valid
//     * in queries as you will never get to serve() method if arguments fail
//     * their parsing.
//     */
//    public final boolean specified() {
//      return _originalArguments.get().containsKey(_name);
//    }
//
//    /** Returns the original value submitted in the form, or empty string if
//     * no value was submitted.
//     */
//    public final String originalValue() {
//      return _originalArguments.get().getProperty(_name,"");
//    }
//
//    /** Returns the query HTML code for the argument. The argument's value() is
//     * used to determine what to put in the form.
//     *
//     * By default, each argument displays a simple text field, with its own
//     * name, inputValue if any and description used as placeholder.
//     *
//     * NOTE Override this method if you want to specify a more elaborate input
//     * method in the HTML query for the argument.
//     */
//    protected String query() {
//      String v = originalValue();
//      if (v.isEmpty()) {
//        T dv = value();
//        if (dv != null)
//          v = dv.toString();
//      }
//      return DOM.textInput(_name, v, description());
//    }
//
//    /** Returns the value of the argument if it is not null. If null, throws the
//     * exception.
//     */
//    public T notNullValue() throws Exception {
//      T v = value();
//      if (v == null)
//        throw new Exception("argument "+_name+" must be defined");
//      return v;
//    }
//
//    /** Returns null if the argument is currently not disabled, otherwise returns
//     * the disabled message (reason).
//     */
//    public final String disabled() {
//      return _disabled.get().getProperty(_name,null);
//    }
//
//    /** Disables the current argument. Supplied message is the reason why the
//     * argument is disabled.
//     */
//    public final void disable(String message) {
//      _disabled.get().put(_name, message);
//    }
//
//    /** Checks that all requirements of the argument are met. If not it should
//     * mark the argument as disabled.
//     */
//    protected void checkRequirements() {
//    }
//
//  }
//
//  // ---------------------------------------------------------------------------
//
//  /** Argument with a simple default value.
//   *
//   * Any argument that can specify its default value (if any) from its
//   * initialization should inherit from this class.
//   *
//   * @param <T>
//   */
//  public abstract class DefaultValueArgument<T> extends Argument<T> {
//
//    private T _defaultValue;
//
//    /** Creates an argument with given name, help, and default value. This
//     * argument is never required.
//     */
//    protected DefaultValueArgument(String name, T defaultValue, String help) {
//      super(name, false, help);
//      _defaultValue = defaultValue;
//    }
//
//    /** Creates an argument with given name and help. This argument is always
//     * required. Should the defaultValue() method on this argument be called,
//     * the result is null.
//     *
//     * NOTE that null cannot be used to determine whether the argument is
//     * required or not, as it may very well be a valid value of the argument
//     * itself.
//     */
//    protected DefaultValueArgument(String name, String help) {
//      super(name, true, help);
//      _defaultValue = null;
//    }
//
//    /** Returns the previously created default value.
//     */
//    @Override protected T defaultValue() {
//      return _defaultValue;
//    }
//  }
//
//  // ===========================================================================
//  // Any defined Argument type should go here:
//  // ===========================================================================
//
//  /** Any string argument.
//   *
//   * Performs *no* checks at all so accepts any string.
//   *
//   * NOTE that unless default value empty string is defined, the string will
//   * not accept an empty string.
//   */
//  public class Str extends DefaultValueArgument<String> {
//
//    /** Creates a required string argument. Does not accept even empty string.
//     */
//    public Str(String name, String help) {
//      super(name, help);
//    }
//
//    /** Creates an optional string argument. Default value must be specified.
//     */
//    public Str(String name, String defaultValue, String help) {
//      super(name, defaultValue, help);
//    }
//
//    /** Parses the string. The string is simple returned.
//     */
//    @Override protected String parse(String input) throws Exception {
//      return input;
//    }
//
//    /** Any string will do, non-empty if required.
//     */
//    @Override public String description() {
//      return _required ? "any nonempty string" : "any string";
//    }
//
//  }
//
//  // ---------------------------------------------------------------------------
//
//  /** Integer argument.
//   *
//   */
//
//  public class Int extends DefaultValueArgument<Integer> {
//
//    public final int _min;
//    public final int _max;
//
//    public Int(String name, String help) {
//      this(name,help, Integer.MIN_VALUE, Integer.MAX_VALUE);
//    }
//
//    public Int(String name, String help, int min, int max) {
//      super(name, help);
//      _min = min;
//      _max = max;
//    }
//
//    public Int(String name, int defaultValue, String help) {
//      this(name, defaultValue, help, Integer.MIN_VALUE, Integer.MAX_VALUE);
//    }
//
//    public Int(String name, int defaultValue, String help, int min, int max) {
//      super(name, defaultValue, help);
//      _min = min;
//      _max = max;
//    }
//
//    @Override protected Integer parse(String input) throws Exception {
//      int result;
//      try {
//        result = Integer.parseInt(input);
//      } catch (NumberFormatException e) {
//        throw new Exception(input+" is not a valid integer");
//      }
//      if ((result<_min) || (result>_max))
//        throw new Exception(input+" is not from "+_min+" to "+_max+" (inclusive)");
//      return result;
//    }
//
//    @Override public String description() {
//      if ((_min == Integer.MIN_VALUE) && (_max == Integer.MAX_VALUE))
//        return "any integer number";
//      return "integer from "+_min+" to "+_max+" (inclusive)";
//    }
//  }
//
//  // ---------------------------------------------------------------------------
//
//  /** Real argument
//   *
//   */
//  public class Real extends DefaultValueArgument<Double> {
//
//    public final Double _min;
//    public final Double _max;
//
//    public Real(String name, String help) {
//      this(name, help, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
//    }
//
//    public Real(String name, String help, double min, double max) {
//      super(name, help);
//      _min = min;
//      _max = max;
//    }
//
//    public Real(String name, double defaultValue, String help) {
//      this(name, defaultValue, help, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
//    }
//
//    public Real(String name, double defaultValue, String help, double min, double max) {
//      super(name, defaultValue, help);
//      _min = min;
//      _max = max;
//    }
//
//    @Override
//    protected Double parse(String input) throws Exception {
//      double result;
//      try {
//        result = Double.parseDouble(input);
//      } catch (NumberFormatException e) {
//        throw new Exception(input+" is not a valid real number");
//      }
//      if ((result<_min) || (result>_max))
//        throw new Exception(input+" is not from "+_min+" to "+_max+" (inclusive)");
//      return result;
//    }
//
//    @Override
//    public String description() {
//      if ((_min == Double.NEGATIVE_INFINITY) && (_max == Double.POSITIVE_INFINITY))
//        return "any real number";
//      return "real number from "+_min+" to "+_max+" (inclusive)";
//    }
//  }
//
//  // ---------------------------------------------------------------------------
//
//  /** Boolean argument.
//   *
//   */
//  public class Bool extends DefaultValueArgument<Boolean> {
//
//    public Bool(String name, String help) {
//      super(name, false, help);
//    }
//
//    public Bool(String name, boolean defaultValue, String help) {
//      super(name, defaultValue, help);
//    }
//
//    @Override protected Boolean parse(String input) throws Exception {
//      if (input.equals("1"))
//        return true;
//      if (input.equals("0"))
//        return false;
//      throw new Exception("value "+input+" is not boolean (1 or 0 accepted only)");
//    }
//
//    @Override public String description() {
//      return super.help();
//    }
//
//    @Override public String help() {
//      return "";
//    }
//
//    @Override protected String query() {
//      return DOM.checkbox(_name, value(), description());
//    }
//  }
//
//  // ---------------------------------------------------------------------------
//
//  public class H2OKey extends DefaultValueArgument<Key> {
//
//    public H2OKey(String name, String help) {
//      super(name, help);
//    }
//
//    public H2OKey(String name, String keyName, String help) {
//      super(name, Key.make(keyName), help);
//    }
//
//    public H2OKey(String name, Key key, String help) {
//      super(name, key, help);
//    }
//
//    @Override protected Key parse(String input) throws Exception {
//      return Key.make(input);
//    }
//
//    @Override public String description() {
//      // TODO what actually is a valid key name? I know I should now, but it is
//      // better to have tests for it:)
//      return "a valid H2O key name";
//    }
//
//  }
//
//  // ---------------------------------------------------------------------------
//
//  private static final String _h2oKeysJavascript =
//            "<script type='text/javascript'>"
//          + "$('#%ID').typeahead({"
//          + "  source:"
//          + "    function(query,process) {"
//          + "      return $.get('%TYPEAHEAD', { filter: query }, function (data) {"
//          + "        return process(data.keys);"
//          + "      });"
//          + "    }"
//          + "});"
//          + "</script>"
//          ;
//
//
//  public class H2OExistingKey extends Argument<Value> {
//
//    // IS ALWAYS REQUIRED
//    public H2OExistingKey(String name, String help) {
//      super(name, true, help);
//    }
//
//    @Override protected Value parse(String input) throws Exception {
//      Key k = Key.make(input);
//      Value v = DKV.get(k);
//      if (v == null)
//        throw new Exception("key "+input+" does not exist");
//      return v;
//    }
//
//    @Override public String description() {
//      return "an existing H2O key";
//    }
//
//    @Override protected String query() {
//      RString js = new RString(_h2oKeysJavascript);
//      js.replace("ID",_name);
//      js.replace("TYPEAHEAD","WWWKeys.json");
//      return super.query() + js.toString();
//    }
//  }
//
//  // ---------------------------------------------------------------------------
//
//
//  public class H2OHexKey extends Argument<ValueArray> {
//
//    // IS ALWAYS REQUIRED
//    public H2OHexKey(String name, String help) {
//      super(name, true, help);
//    }
//
//    @Override protected ValueArray parse(String input) throws Exception {
//      Key k = Key.make(input);
//      Value v = DKV.get(k);
//      if (v == null)
//        throw new Exception("key "+input+" does not exist!");
//      if (!(v instanceof ValueArray))
//        throw new Exception("key "+input+" does not point to a HEX file");
//      ValueArray va = (ValueArray) v;
//      if (va.num_cols()==0)
//        throw new Exception("key "+input+" does not point to a HEX file");
//      return va;
//    }
//
//    @Override public String description() {
//      // TODO how do we call these keys for customers?
//      return "an existing hex key";
//    }
//
//    @Override protected String query() {
//      RString js = new RString(_h2oKeysJavascript);
//      js.replace("ID",_name);
//      js.replace("TYPEAHEAD","WWWHexKeys.json");
//      return super.query() + js.toString();
//    }
//  }
//
//  // ---------------------------------------------------------------------------
//
//  /** Argument for a key column.
//   *
//   * Given a specific key.
//   *
//   */
//  public class H2OKeyCol extends DefaultValueArgument<Integer> {
//    protected final H2OHexKey _key;
//
//    public H2OKeyCol(H2OHexKey key, String name, String help) {
//      super(name,help);
//      _key = key;
//    }
//
//    public H2OKeyCol(H2OHexKey key, String name, int defaultValue, String help) {
//      super(name, defaultValue, help);
//      _key = key;
//    }
//
//    @Override protected Integer parse(String value) throws Exception {
//      ValueArray ary = _key.notNullValue();
//      for (int i = 0; i < ary.num_cols(); ++i)
//        if (ary.col_name(i).equals(value))
//          return i;
//      try {
//        int i = Integer.parseInt(value);
//        if ((i<0) || (i>=ary.num_cols()))
//          throw new Exception("Column index "+i+" out of range <0 , "+ary.num_cols()+") of columns for key "+ary._key);
//        return i;
//      } catch (NumberFormatException e) {
//        throw new Exception(value+" does not name any column in key "+ary._key);
//      }
//    }
//
//    @Override public String description() {
//      return "Index of the column, or the column name for key specified by argument "+_key._name;
//    }
//
//    /** Checks that all requirements of the argument are met. If not it should
//     * mark the argument as disabled.
//     */
//    @Override protected void checkRequirements() {
//      if ((_key.disabled() != null)
//        || (_key.value() == null))
//        disable("Argument "+_key._name+" must be specified in order to edit.");
//    }
//  }
//
//  // ---------------------------------------------------------------------------
//
//  public class H2OCategoryDoubles extends Argument<double[]> {
//
//    public final double _defaultValue;
//    public final double _min;
//    public final double _max;
//    public final H2OHexKey _key;
//    public final H2OKeyCol _col;
//
//    public H2OCategoryDoubles(H2OHexKey key, H2OKeyCol col, String name, double defaultValue, String help, double min, double max) {
//      super(name, false, help);
//      _key = key;
//      _col = col;
//      _defaultValue = defaultValue;
//      _min = min;
//      _max = max;
//    }
//
//    @Override protected double[] parse(String input) throws Exception {
//      ValueArray ary = _key.notNullValue();
//      int col = _col.notNullValue();
//      double[] result = determineClassWeights(input,ary,col,4096);
//      return result;
//    }
//
//    @Override public String description() {
//      return "comma separated list of assignment to col names or col idxs";
//    }
//
//    @Override protected double[] defaultValue() {
//      try {
//        ValueArray ary = _key.notNullValue();
//        int col = _col.notNullValue();
//        String[] names = determineColumnClassNames(ary,col,4096);
//        double result[] = new double[names.length];
//        for (int i = 0; i < result.length; ++i)
//          result[i] = _defaultValue;
//        return result;
//      } catch (Exception e) {
//        return null;
//      }
//    }
//
//    /** Checks that all requirements of the argument are met. If not it should
//     * mark the argument as disabled.
//     */
//    @Override protected void checkRequirements() {
//      if ((_key.disabled() != null)
//              || (_key.value() == null)
//              || (_col.disabled() != null)
//              || (_col.value() != null))
//        disable("Arguments "+_key._name+" and "+_col._name+" must be specified in order to edit.");
//    }
//
//    protected String[] determineColumnClassNames(ValueArray ary, int classColIdx, int maxClasses) throws Exception {
//      int arity = ary.col_enum_domain_size(classColIdx);
//      if (arity == 0) {
//        int min = (int) ary.col_min(classColIdx);
//        if (ary.col_min(classColIdx) != min)
//          throw new Exception("Only integer or enum columns can be classes!");
//        int max = (int) ary.col_max(classColIdx);
//        if (ary.col_max(classColIdx) != max)
//          throw new Exception("Only integer or enum columns can be classes!");
//        if (max - min > maxClasses) // arbitrary number
//          throw new Exception("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
//        String[] result = new String[max-min+1];
//        for (int i = 0; i <= max - min; ++i)
//          result[i] = String.valueOf(min+i);
//        return result;
//      } else {
//        return  ary.col_enum_domain(classColIdx);
//      }
//    }
//
//
//    public double[] determineClassWeights(String source, ValueArray ary, int classColIdx, int maxClasses) throws Exception {
//      assert classColIdx>=0 && classColIdx < ary.num_cols();
//      // determine the arity of the column
//      HashMap<String,Integer> classNames = new HashMap();
//      String[] names = determineColumnClassNames(ary,classColIdx,maxClasses);
//      for (int i = 0; i < names.length; ++i)
//        classNames.put(names[i],i);
//      if (source.isEmpty())
//        return null;
//      double[] result = new double[names.length];
//      for (int i = 0; i < result.length; ++i)
//        result[i] = 1;
//      // now parse the given string and update the weights
//      int start = 0;
//      byte[] bsource = source.getBytes();
//      while (start < bsource.length) {
//        while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
//        String className;
//        double classWeight;
//        int end = 0;
//        if (bsource[start] == ',') {
//          ++start;
//          end = source.indexOf(',',start);
//          className = source.substring(start,end);
//          ++end;
//
//        } else {
//          end = source.indexOf('=',start);
//          className = source.substring(start,end);
//        }
//        start = end;
//        while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
//        if (bsource[start]!='=')
//          throw new Exception("Expected = after the class name.");
//        ++start;
//        end = source.indexOf(',',start);
//        if (end == -1) {
//          classWeight = Double.parseDouble(source.substring(start));
//          start = bsource.length;
//        } else {
//          classWeight = Double.parseDouble(source.substring(start,end));
//          start = end + 1;
//        }
//        if (!classNames.containsKey(className))
//          throw new Exception("Class "+className+" not found!");
//        result[classNames.get(className)] = classWeight;
//      }
//      return result;
//    }
//
//  }
//
//  // ---------------------------------------------------------------------------
//
//  public class H2OKeyCols extends DefaultValueArgument<int[]> {
//
//    public final H2OHexKey _key;
//
//    public H2OKeyCols(H2OHexKey key, String name, int[] defaultValue, String help) {
//      super(name,defaultValue,help);
//      _key = key;
//    }
//
//    private int colToInt(ValueArray ary, String colName) throws Exception {
//      colName = colName.trim();
//      for (int i = 0; i < ary.num_cols(); ++i)
//        if (ary.col_name(i).equals(colName))
//          return i;
//      try {
//        int i = Integer.parseInt(colName);
//        if ((i>=0) && ( i < ary.num_cols()))
//          return i;
//      } catch (NumberFormatException e) {
//      }
//      throw new Exception(colName+" is not a valid column name or column index");
//    }
//
//    @Override protected int[] parse(String input) throws Exception {
//      ValueArray ary = _key.notNullValue();
//      Set<Integer> cols = new HashSet();
//      ARGS:
//      for (String s : input.split(",")) {
//        int i = colToInt(ary,s);
//        if (cols.contains(i))
//          throw new Exception("Column "+s+" already specified");
//        cols.add(i);
//      }
//      int[] result = new int[cols.size()];
//      int i = 0;
//      for (Integer j : cols)
//        result[i++] = j;
//      return result;
//    }
//
//    @Override public String description() {
//      return "comma separated list of columns (numbers or names)";
//    }
//
//  }






}
