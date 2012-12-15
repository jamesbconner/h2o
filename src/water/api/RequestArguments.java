
package water.api;

import java.util.*;
import water.*;
import water.web.RString;

/** All arguments related classes are defined in this guy.
 *
 * Argument is the base class for all arguments, which then branches to
 * different still abstract subclasses that specify how are any given HTML input
 * elements being drawn.
 *
 * From these a proper Arguments that define particular value types are then
 * subclassed.
 *
 * When an argument is created, its pointer is stored in the _arguments array
 * list so that the request knows how many arguments and in which order it has.
 *
 * Because request objects and therefore also argument objects one per
 * application, while the codepath can be multithreaded (server decides this),
 * the argument state is not preserved in the argument itself, but in the
 * Record static object that is kept thread local and must be properly
 * initialized at each iteration by calling reset() method on the argument.
 *
 * See the respective classes for more details.
 *
 * NOTE add more arguments to this class as they are needed and keep them here.
 *
 * @author peta
 */
public class RequestArguments extends RequestStatics {

  /** List of arguments for the request. Automatically filled in by the argument
   * constructors.
   */
  protected ArrayList<Argument> _arguments = new ArrayList();

  // ---------------------------------------------------------------------------

  /** Argument state record.
   *
   * Contains all state required for the argument and a few functions to operate
   * on the state.
   */
  protected static class Record<T> {

    /** Determines the original input value of the argument. null if the value
     * was not supplied, or was empty. Retains the original value even if the
     * argument value is wrong.
     */
    public String _originalValue = null;

    /** Parsed value. If the parse was successful, or default value if the
     * checking failed, or the argument is not required and was missing. Note
     * that default value may very well be null and thus you cannot check this
     * for null of determine validity.
     */
    public T _value = null;

    /** Reason why the argument is disabled, or null if it is enabled. A
     * disabled argument cannot be edited by the user yet.
     */
    public String _disabledReason = null;

    /** True if the argument's value stored in _value is valid, that is either
     * correctly parsed, or not present and default value used. Note that if
     * checking fails, the defaultValue is stored in _value, but _valid is
     * false.
     */
    public boolean _valid = false;

    /** Returns true if the argument is disabled.
     */
    public boolean disabled() {
      return _disabledReason != null;
    }

    /** Returns true if the argument is valid.
     */
    public boolean valid() {
      return _valid;
    }

    /** Returns if the argument is specified by user. returns true only if it is
     * valid and parsing the argument was successful.
     * @return
     */
    public boolean specified() {
      return valid() && _originalValue != null;
    }
  }

  // A string used to display the query element part of the argument
  protected static final String _queryHtml =
            "  <dl class='dl-horizontal'>"
          + "    <dt>%ASTERISK %NAME</dt>"
          + "    <dd>"
          + "      %ELEMENT"
          + "    </dd>"
          + "  </dl>"
          ;

  // ===========================================================================
  // Argument
  // ===========================================================================

  public abstract class Argument<T> {

    /** As with request's _requestHelp, this provides the extended help that
     * will be displayed on the help and wiki pages. Specify this in the
     * particular request constructor.
     */
    public String _requestHelp;

    /** Override this method to provide parsing of the input string to the Java
     * expected value. The input is guaranteed to be non-empty when this method
     * is called and all prerequisities are guaranteed to be valid before this
     * method is called.
     */
    protected abstract T parse(String input) throws IllegalArgumentException;

    /** Returns the default value of the argument. Note that the method will be
     * called also on required arguments, in which case it is ok return null.
     *
     * It is kept abstract because defining a proper default value might be
     * tricky and in many case you do not want it to be null. Overriding it
     * always makes you think:)
     */
    protected abstract T defaultValue();

    /** Returns the javascript code that will be executed when the query is
     * loaded that associates the given callback JS function with the on change
     * event of the input. This method is only called if the element should
     * refresh the webpage upon its change.
     */
    protected abstract String jsRefresh(String callbackName);

    /** Returns the javascript code that will be executed when the value of
     * the argument is to be determined. It must contain a return statement,
     * that returns the string that should be sent back to the request for the
     * given arhument.
     */
    protected abstract String jsValue();

    /** If there is any additional javascript that should be dumped to the
     * query page, it should be defined here. Please follow chaining rules.
     */
    protected String jsAddons() {
      return "";
    }

    /** Returns the HTML elements of the argument query only. This should return
     * the elements in HTML that will be used to enter the value. For instance
     * the input text, selection, etc.
     */
    protected abstract String queryElement();

    /** Returns the query description. This is a concise description of a
     * correct value for the argument. generally used as a placeholder in the
     * html query elements.
     */
    protected abstract String queryDescription();

    /** Any query addons can be specified here. These will be displayed with
     * the query html code and should be used for instance for default value
     * calculators, etc.
     */
    protected String queryAddons() {
      return "";
    }

    /** Returns the html query for the given argument, including the full
     * formatting. That means not only the queryElement, but also the argument
     * name in front of it, etc. Required arguments are prefixed with red
     * asterisk.
     *
     * You may want to override this if you wont different form layouts to be
     * present.
     */
    protected String query() {
      RString result = new RString(_queryHtml);
      result.replace("ID",_name);
      result.replace("NAME", JSON2HTML(_name));
      if (disabled())
        result.replace("ELEMENT","<div class='alert alert-notice'>"+record()._disabledReason+"</div>");
      else
        result.replace("ELEMENT",queryElement());
      if (_required)
        result.replace("ASTERISK","<span style='color:#ff0000'>* </span>");
      return result.toString();
    }

    /** Creates the request help page part for the given argument. Displays its
     * JSON name, query name (the one in HTML), value type and the request help
     * provided by the argument.
     */
    public String requestHelp() {
      StringBuilder sb = new StringBuilder();
      sb.append("<h4>"+_name+"</h4>");
      sb.append("<p>"+queryDescription()+"</p>");
      sb.append("<p>"+_requestHelp+"</p>");
      return sb.toString();
    }

    /** Name of the argument. This must correspond to the name of the JSON
     * request argument.
     */
    public final String _name;

    /** True if the argument is required, false if it may be skipped.
     */
    public final boolean _required;

    /** True if change of the value in the query controls should trigger an
     * automatic refresh of the query form.
     *
     * This is set by the setrefreshOnChange() method. It is automatically set
     * for any controls that are prerequisites for other controls and can be
     * manually select for other controls by users (do it in the request
     * constructor).
     */
    private boolean _refreshOnChange;

    /** List of all prerequisite arguments for the current argument. All the
     * prerequisite arguments must be created before the current argument.
     */
    private ArrayList<Argument<T>> _prerequisities = null;

    /** The thread local argument state record. Must be initialized at the
     * beginning of each request before it can be used.
     */
    private ThreadLocal<Record> _argumentRecord = new ThreadLocal();

    /** Creates the argument of given name. Also specifies whether the argument
     * is required or not. This cannot be changed later.
     */
    protected Argument(String name, boolean required) {
      _name = name;
      _required = required;
      _refreshOnChange = false;
      _arguments.add(this);
    }

    /** Adds the given argument as a prerequisite. This means that current
     * argument will not be checked and/or reported in queries as a control form
     * unless all its prerequisite arguments are in a valid state. (the argument
     * will be disabled if not all its prerequisites are satisfied).
     */
    protected final void addPrerequisite(Argument arg) {
      if (_prerequisities == null)
        _prerequisities = new ArrayList();
      _prerequisities.add(arg);
      arg.setRefreshOnChange();
    }

    /** Returns the thread local argument state record.
     */
    protected final Record<T> record() {
      return _argumentRecord.get();
    }

    /** Disables the argument with given reason. If the argument is already
     * disabled, its reason is overwritten by the new one.
     *
     * NOTE disable(null) effectively enables the argument, that is why the
     * assert!
     */
    public final void disable(String reason) {
      assert (reason != null);
      record()._disabledReason = reason;
    }

    /** Returns whether the argument is disabled or not.
     */
    public final boolean disabled() {
      return record().disabled();
    }

    /** Makes the argument refresh the query page on its change automatically.
     * If you want this behavior to be disabled for the argument, overwrite this
     * method to error.
     */
    protected void setRefreshOnChange() {
      _refreshOnChange = true;
    }

    /** Returns true if the argument refreshes the query automatically on its
     * change.
     */
    public boolean refreshOnChange() {
      return _refreshOnChange;
    }

    /** Returns true if the argument is valid. Valid means specified by user
     * and parsed properly, or not required and not specified.
     */
    public final boolean valid() {
      return record().valid();
    }

    /** Returns true of the argument is specified by the user. That is if the
     * argument value was submitted by the user and parsed correctly.
     */
    public final boolean specified() {
      return record().specified();
    }

    /** Returns the value of the argument. This is either the value parsed, if
     * specified, or defaultValue. Note that default value is returned also for
     * invalid arguments.
     */
    public final T value() {
      return record()._value;
    }

    /** Resets the argument by creating it a new thread local state. Everything
     * is null and the argument is not valid.
     */
    public final void reset() {
      _argumentRecord.set(new Record());
    }

    /** Checks that the argument supplied is correct. This method is called for
     * each argument and is given the HTTP supplied argument value. If the value
     * was not supplied, input contains an empty string.
     *
     * The argument must already be reseted before calling this method.
     *
     * If the argument is disabled, the function does not do anything except
     * setting the original value in the record.
     *
     * If the prerequisites of the argument are not all valid, then the argument
     * is disabled and function returns.
     *
     * Then the argument is parsed if provided, or an error thrown if the input
     * is empty and the argument is required.
     *
     * At the end of the function the value is either the result of a successful
     * parse() call or a defaultValue no matter what the state of the argument
     * is.
     */
    public void check(String input) throws IllegalArgumentException {
      // get the record -- we assume we have been reset properly
      Record record = record();
      // check that the input is canonical == value or null and store it to the
      // record
      if (input.isEmpty())
        input = null;
      record._originalValue = input;
      // there is not much to do if we are disabled
      if (record.disabled()) {
        record._value = defaultValue();
        return;
      }
      // check that we have all prerequisities properly initialized
      if (_prerequisities != null) {
        for (Argument dep : _prerequisities)
          if (!dep.valid()) {
            record._disabledReason = "Not all prerequisite arguments have been supplied: "+dep._name;
            record._value = defaultValue();
            return;
          }
      }
      // if input is null, throw if required, otherwise use the default value
      if (input == null) {
        if (_required)
          throw new IllegalArgumentException("Argument "+_name+" is required, but not specified");
        record._value = defaultValue();
        record._valid = true;
      // parse the argument, if parse throws we will still be invalid correctly
      } else {
        try {
          record._value = parse(input);
          record._valid = true;
        } catch (Exception e) {
          record._value = defaultValue();
          throw e;
        }
      }
    }
  }

  // ===========================================================================
  // InputText
  // ===========================================================================

  /** Argument that uses simple text input to define its value.
   *
   * This is the simplest argument. Uses the classic input element. All
   * functionality is supported.
   *
   * @param <T>
   */
  public abstract class InputText<T> extends Argument<T> {

    public InputText(String name, boolean required) {
      super(name, required);
    }

    /** A query element is the default HTML form input.
     *
     * The id of the element is the name of the argument. Placeholder is the
     * query description and the value is filled in either as the value
     * submitted, or as the toString() method on defaultValue.
     */
    @Override protected String queryElement() {
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

    /** JS refresh is a default jQuery hook to the change() method.
     */
    @Override protected String jsRefresh(String callbackName) {
      return "$('#"+_name+"').change("+callbackName+");";
    }

    /** JS value is the simple jQuery val() method.
     */
    @Override protected String jsValue() {
      return "return $('#"+_name+"').val();";
    }
  }

  // ===========================================================================
  // TypeaheadInputText
  // ===========================================================================

  // typeahead assignment js
  private static final String _typeahead =
            "$('#%ID').typeahead({"
          + "  source:"
          + "    function(query,process) {"
          + "      return $.get('%HREF', { filter: query, limit: %LIMIT }, function (data) {"
          + "        return process(data.%DATA_NAME);"
          + "      });"
          + "    }"
          + "});\n"
          ;

  /** Typeahead enabled text input.
   *
   * Typeahead is enabled using the jQuery typeahead plugin. You must specify
   * the JSON request which provides the typeahead, and the data name in the
   * response that contains the array of strings corresponding to the typeahead
   * options. Optionally you can specify the typeahead limit (how many options
   * will be displayed), which is 1024 by default.
   *
   * The typeahead json request must take Str argument filter and Int optional
   * argument limit.
   */
  public abstract class TypeaheadInputText<T> extends InputText<T> {

    /** href of the json request supplying the typeahead values.
     */
    protected final String _typeaheadHref;
    /** Name of the JSON attribute in the request that contains the typahead
     * values. It must be an array of string primitives.
     */
    protected final String _typeaheadDataName;

    /** Typeahead limit. If more than this limit options will be available, the
     * typeahead will be disabled.
     */
    protected final int _typeaheadLimit;

    /** Creates the typeahead. Default limit is 1024.
     */
    protected TypeaheadInputText(String name, boolean required, String href, String dataName) {
      this(name, required, href, dataName, 1024);
    }

    /** Creates the typeahead.
     */
    protected TypeaheadInputText(String name, boolean required, String href, String dataName, int limit) {
      super(name, required);
      _typeaheadHref = href;
      _typeaheadDataName = dataName;
      _typeaheadLimit = limit;
    }

    /** Adds the json to hook initialize the typeahead functionality. It is
     * jQuery typeahead plugin standard initialization with async filler.
     */
    @Override protected String jsAddons() {
      RString s = new RString(_typeahead);
      s.replace("ID", _name);
      s.replace("HREF", _typeaheadHref);
      s.replace("LIMIT", _typeaheadLimit);
      s.replace("DATA_NAME", _typeaheadDataName);
      return super.jsAddons()+s.toString();
    }
  }

  // ===========================================================================
  // InputCheckBox
  // ===========================================================================

  /** A boolean argument that is represented as the checkbox.
   *
   * The only allowed values for a boolean checkbox are "0" for false, "1" for
   * true. If the argument is not required, then default value will be used.
   *
   * Please note that due to the nature of a checkbox, the html query will
   * always specify this argument to its default value, or to false if the user
   * did not specify it explicitly.
   */
  public abstract class InputCheckBox extends Argument<Boolean> {

    /** Default value.
     */
    public final Boolean _defaultValue;

    /** Creates the argument with specified default value.
     */
    public InputCheckBox(String name, boolean defaultValue) {
      super(name, false); // checkbox is never required
      _defaultValue = defaultValue;
    }

    /** Creates the argument as required one. This has only effect on JSON, for
     * HTML it means the default value is false effectively.
     */
    public InputCheckBox(String name) {
      super(name, true);
      _defaultValue = null;
    }

    /** Parses the value. 1 to true and 0 to false. Anything else is an error.
     */
    @Override public Boolean parse(String input) {
      if (input.equals("1"))
        return true;
      if (input.equals("0"))
        return false;
      throw new IllegalArgumentException(input+" is not valid boolean value. Only 1 and 0 are allowed.");
    }

    /** Displays the query element. This is just the checkbox.
     */
    @Override protected String queryElement() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, use the provided one
      if (value == null) {
        Boolean v = defaultValue();
        value = ((v == null) || (v == false)) ? "" : "1" ;
      }
      return "<input value='1' class='span5' type='checkbox' name='"+_name+"' id='"+_name+"' "+ (value.equals("1") ? (" checked />") : "/>");
    }

    /** Refresh only taps to jQuery change event.
     */
    @Override protected String jsRefresh(String callbackName) {
      return "$('#"+_name+"').change('"+callbackName+"');";
    }

    /** Returns 1 if the checkbox is checked and 0 otherwise.
     */
    @Override protected String jsValue() {
      return "return $('#"+_name+"').val() == '1' ? '1' : '0';";
    }

    /** Returns the default value.
     */
    @Override protected Boolean defaultValue() {
      return _defaultValue;
    }
  }

  // ===========================================================================
  // InputSelect
  // ===========================================================================

  /** Select element from the list of options.
   *
   * Array of values and arrays of names can be specified together with the
   * selected element's value.
   */
  public abstract class InputSelect<T> extends Argument<T> {


    /** Override this method to provide the values for the options. These will
     * be the possible values returned by the form's input and should be the
     * possible values for the JSON argument.
     */
    protected abstract String[] selectValues();

    /** Returns which value should be selected. This is *not* the default value
     * itself, as the default values may be of any type, but the input value
     * that should be selected in the browser.
     */
    protected abstract String selectedItemValue();

    /** Override this method to determine the value names, that is the names
     * displayed in the browser. Return null, if the value strings should be
     * used (this is default behavior).
     */
    protected String[] selectNames() {
      return null;
    }

    /** Constructor just calls super.
     */
    public InputSelect(String name, boolean required) {
      super(name, required);
    }

    /** Displays the query element. It is a select tag with option tags inside.
     * If the argument is required then additional empty value is added with
     * name "Please select..." that ensures that the user selects actual value.
     */
    @Override protected String queryElement() {
      StringBuilder sb = new StringBuilder();
      sb.append("<select id='"+_name+"' name='"+_name+"'>");
      String selected = selectedItemValue();
      String[] values = selectValues();
      String[] names = selectNames();
      if (names == null)
        names = values;
      assert (values.length == names.length);
      if (_required)
          sb.append("<option value=''>Please select...</option>");
      for (int i = 0 ; i < values.length; ++i) {
        if (values[i].equals(selected))
          sb.append("<option value='"+values[i]+"' selected>"+names[i]+"</option>");
        else
          sb.append("<option value='"+values[i]+"'>"+names[i]+"</option>");
      }
      sb.append("</select>");
      return sb.toString();
    }

    /** Refresh is supported using standard jQuery change event.
     */
    @Override protected String jsRefresh(String callbackName) {
      return "$('#"+_name+"').change('"+callbackName+"');";
    }

    /** Get value is supported by the standard val() jQuery function.
     */
    @Override protected String jsValue() {
      return "return $('#"+_name+"').val();";
    }

  }

  // ===========================================================================
  // UserDefinedArguments
  //
  // Place your used defined arguments here.
  //
  // ===========================================================================

  // ---------------------------------------------------------------------------
  // Str
  // ---------------------------------------------------------------------------

  /** A string value.
   *
   * Any string can be a proper value. If required, empty string is not allowed.
   */
  public class Str extends InputText<String> {

    public final String _defaultValue;

    public Str(String name) {
      super(name,true);
      _defaultValue = null;
    }

    public Str(String name, String defaultValue) {
      super(name, false);
      _defaultValue = defaultValue;
    }

    @Override protected String parse(String input) throws IllegalArgumentException {
      return input;
    }

    @Override protected String defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return _required ? "any non-empty string" : "any string";
    }

  }

  // ---------------------------------------------------------------------------
  // Int
  // ---------------------------------------------------------------------------

  public class Int extends InputText<Integer> {

    public final Integer _defaultValue;

    public final int _min;
    public final int _max;

    public Int(String name) {
      this(name, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public Int(String name, int min, int max) {
      super(name,true);
      _defaultValue = null;
      _min = min;
      _max = max;
    }

    public Int(String name, Integer defaultValue) {
      this(name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public Int(String name, Integer defaultValue, int min, int max) {
      super(name,false);
      _defaultValue = defaultValue;
      _min = min;
      _max = max;
    }

    @Override protected Integer parse(String input) throws IllegalArgumentException {
      try {
        int i = Integer.parseInt(input);
        if ((i< _min) || (i > _max))
          throw new IllegalArgumentException("Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
        return i;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Value "+input+" is not a valid integer.");
      }
    }

    @Override protected Integer defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return ((_min == Integer.MIN_VALUE) && (_max == Integer.MAX_VALUE))
              ? "integer value"
              : "integer from "+_min+" to "+_max;
    }
  }

  // ---------------------------------------------------------------------------
  // Real
  // ---------------------------------------------------------------------------

  public class Real extends InputText<Double> {

    public final Double _defaultValue;

    public final double _min;
    public final double _max;

    public Real(String name) {
      this(name, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public Real(String name, double min, double max) {
      super(name,true);
      _defaultValue = null;
      _min = min;
      _max = max;
    }

    public Real(String name, Double defaultValue) {
      this(name, defaultValue, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public Real(String name, Double defaultValue, double min, double max) {
      super(name,true);
      _defaultValue = defaultValue;
      _min = min;
      _max = max;
    }

    @Override protected Double parse(String input) throws IllegalArgumentException {
      try {
        double i = Double.parseDouble(input);
        if ((i< _min) || (i > _max))
          throw new IllegalArgumentException("Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
        return i;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Value "+input+" is not a valid real number.");
      }
    }

    @Override protected Double defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return ((_min == Double.NEGATIVE_INFINITY) && (_max == Double.POSITIVE_INFINITY))
              ? "integer value"
              : "integer from "+_min+" to "+_max;
    }
  }

  // ---------------------------------------------------------------------------
  // H2OKey
  // ---------------------------------------------------------------------------

  public class H2OKey extends InputText<Key> {

    public final Key _defaultValue;

    public H2OKey(String name) {
      super(name, true);
      _defaultValue = null;
    }

    public H2OKey(String name, String keyName) {
      this(name, Key.make(keyName));
    }

    public H2OKey(String name, Key key) {
      super(name, false);
      _defaultValue = key;
    }

    @Override protected Key parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      return k;
    }

    @Override protected Key defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return "valid H2O key";
    }

  }

  // ---------------------------------------------------------------------------
  // H2OExistingKey
  // ---------------------------------------------------------------------------

  public class H2OExistingKey extends TypeaheadInputText<Value> {

    public final Key _defaultValue;

    public H2OExistingKey(String name) {
      super(name, true, "WWWKeys.json",JSON_KEYS);
      _defaultValue = null;
    }

    public H2OExistingKey(String name, String keyName) {
      this(name, Key.make(keyName));
    }

    public H2OExistingKey(String name, Key key) {
      super(name, false,"WWWKeys.json", JSON_KEYS);
      _defaultValue = key;
    }

    @Override protected Value parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)
        throw new IllegalArgumentException("Key "+input+" not found!");
      return v;
    }

    @Override protected Value defaultValue() {
      return DKV.get(_defaultValue);
    }

    @Override protected String queryDescription() {
      return "an existing H2O key";
    }

  }

  // ---------------------------------------------------------------------------
  // StringListArgument
  // ---------------------------------------------------------------------------

  // NO EMPTY string in values
  public class StringList extends InputSelect<String> {

    public final String[] _values;

    public final int _defaultIndex;

    public StringList(String name, String[] values) {
      super(name, true);
      _values = values;
      _defaultIndex = -1;
    }

    public StringList(String name, String[] values, int defaultIndex) {
      super(name, false);
      _values = values;
      _defaultIndex = defaultIndex;
    }

    @Override  protected String[] selectValues() {
      return _values;
    }

    @Override protected String selectedItemValue() {
      if (_required && (!valid()))
        return "";
      return value();
    }

    @Override protected String parse(String input) throws IllegalArgumentException {
      for (String s : _values)
        if (s.equals(input))
          return input;
      throw new IllegalArgumentException("Invalid value "+input+", only "+Arrays.toString(_values)+" allowed");
    }

    @Override
    protected String defaultValue() {
      if (_defaultIndex == -1)
        return null;
      return _values[_defaultIndex];
    }

    @Override protected String queryDescription() {
      return "any of "+Arrays.toString(_values);
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
