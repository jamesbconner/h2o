
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;

/** Builders & response object.
 *
 * It just has a stuff of simple builders that walk through the JSON response
 * and format the stuff into basic html. Understands simplest form of tables,
 * objects and elements.
 *
 * Also defines the response object that contains the response JSON, response
 * state, other response related automatic variables (timing, etc) and the
 * custom builders.
 *
 * TODO work in progress.
 *
 * @author peta
 */
public class RequestBuilders extends RequestQueries {

  /** Builds the HTML for the given response.
   *
   * This is the root of the HTML. Should display all what is needed, including
   * the status, timing, etc. Then call the recursive builders for the
   * response's JSON.
   */
  protected String build(Response response) {
    StringBuilder sb = new StringBuilder();
    sb.append("<h3>"+getClass().getSimpleName()+" response:</h3>");
    Builder builder = response.getBuilderFor("");
    if (builder == null)
      builder = OBJECT_BUILDER;
    sb.append(builder.build(response,response._response,""));
    return sb.toString();
  }

  public static final Builder OBJECT_BUILDER = new ObjectBuilder();
  public static final Builder ARRAY_BUILDER = new ArrayBuilder();
  public static final Builder ARRAY_ROW_BUILDER = new ArrayRowBuilder();
  public static final Builder ELEMENT_BUILDER = new ElementBuilder();
  public static final Builder ARRAY_ROW_ELEMENT_BUILDER = new ArrayRowElementBuilder();
  public static final Builder ARRAY_ROW_SINGLECOL_BUILDER = new ArrayRowSingleColBuilder();

  /** This is a response class for the JSON.
   *
   * Instead of simply returning a JsonObject, each request returns a new
   * response object that it must create. This is (a) cleaner (b) more
   * explicit and (c) allows to specify response states used for proper
   * error reporting, stateless and statefull processed and so on, and (d)
   * allows specification of HTML builder hooks in a nice clean interface.
   *
   * The work pattern should be that in the serve() method, a JsonObject is
   * created and populated with the variables. Then if any error occurs, an
   * error response should be returned.
   *
   * Otherwise a correct state response should be created at the end from the
   * json object and returned.
   *
   * TODO Work in progress. Please do not change.
   */
  public static final class Response {

    /** Status of the response.
     *
     * Defines the state of the response so that it can be nicely reported to
     * the user in either in JSON or in HTML in a meaningful manner.
     */
    public static enum Status {
      done, ///< Indicates that the request has completed and no further actuion from the user is required
      poll, ///< Indicates that the same request should be repeated to see some progress
      redirect, ///< Indicates that the request was successful, but new request must be filled to obtain results
      error ///< The request was an error.
    }

    /** Status of the request.
     */
    private final Status _status;

    /** Response object for JSON requests.
     */
    private final JsonObject _response;

    /** Private constructor creating the request with given type and response
     * JSON object.
     *
     * Use the static methods to construct the response objects. (looks better
     * when we have a lot of them).
     */
    private Response(Status status, JsonObject response) {
      _status = status;
      _response = response;
    }

    /** Returns new error response with given error message.
     */
    public static Response error(String message) {
      JsonObject obj = new JsonObject();
      obj.addProperty(JSON_ERROR,message);
      return new Response(Status.error,obj);
    }

    /** Returns new done response with given JSON response object.
     */
    public static Response done(JsonObject response) {
      return new Response(Status.done, response);
    }

    /** Hashmap of custom builders for JSON elements when converting to HTML
     * automatically.
     */
    private HashMap<String,Builder> _builders = new HashMap();

    /** Associates a given builder with the specified JSON context. JSON context
     * is a dot separated path to the JSON object/element starting from root.
     *
     * One exception is an array row element, which does not really have a
     * distinct name in JSON and is thus identified as the context name of the
     * array + "_ROW" appended to it.
     *
     * The builder object will then be called to build the HTML for the
     * particular JSON element. By wise subclassing of the preexisting builders
     * and changing their behavior an arbitrarily complex webpage can be
     * created.
     */
    public void setBuilder(String contextName, Builder builder) {
      _builders.put(contextName, builder);
    }

    /** Returns the builder for given JSON context element. Null if not found
     * in which case a default builder object will be used. These default
     * builders are specified by the builders themselves.
     */
    protected Builder getBuilderFor(String contextName) {
      return _builders.get(contextName);
    }

    /** Returns the JSONified version of the request. At the moment just
     * returns the response.
     *
     * TODO change this to reflect the status properly.
     * @return
     */
    public JsonObject toJson() {
      return _response;
    }

    /** Returns the error of the request object if any. Returns null if the
     * response is not in error state.
     */
    public String error() {
      if (_status != Status.error)
        return null;
      return _response.get(JSON_ERROR).getAsString();
    }

  }

  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  /** An abstract class to build the HTML page automatically from JSON.
   *
   * The idea is that every JSON element in the response structure (dot
   * separated) may be a unique context that might be displayed in a different
   * way. By creating specialized builders and assigning them to the JSON
   * element contexts you can build arbitrarily complex HTML page.
   *
   * The basic builders for elements, arrays, array rows and elements inside
   * array rows are provided by default.
   *
   * Each builder can also specify default builders for its components to make
   * sure for instance that tables in arrays do not recurse and so on.
   */
  public static abstract class Builder {
    /** Override this method to provide HTML for the given json element.
     *
     * The arguments are the response object, the element whose HTML should be
     * produced and the contextName of the element.
     */
    public abstract String build(Response response, JsonElement element, String contextName);

    /** Adds the given element name to the existing context. Dot concatenates
     * the names.
     */
    public static String addToContext(String oldContext, String name) {
      if (oldContext.isEmpty())
        return name;
      return oldContext+"."+name;
    }

    /** For a given context returns the element name. That is the last word
     * after a dot, or the full string if dot is not present.
     */
    public static String elementName(String context) {
     int idx = context.lastIndexOf(".");
     return context.substring(idx+1);
    }

    /** Returns the default builders.
     *
     * These are element builder, object builder and array builder.
     */
    public Builder defaultBuilder(JsonElement element) {
      if (element instanceof JsonArray)
        return ARRAY_BUILDER;
      else if (element instanceof JsonObject)
        return OBJECT_BUILDER;
      else
        return ELEMENT_BUILDER;
    }

  }

  // ---------------------------------------------------------------------------
  // ObjectBuilder
  // ---------------------------------------------------------------------------

  /** Object builder.
   *
   * By default objects are displayed as a horizontal dl elements with their
   * heading preceding any of the values. Methods for caption, header,
   * footer as well as element building are provided so that the behavior can
   * easily be customized.
   */
  public static class ObjectBuilder extends Builder {

    /** Displays the caption of the object.
     */
    public String caption(JsonObject object, String objectName) {
      return objectName.isEmpty() ? "" : "<h4>"+objectName+"</h4>";
    }

    /** Returns the header of the object.
     *
     * That is any HTML displayed after caption and before any object's
     * contents.
     */
    public String header(JsonObject object, String objectName) {
      return "";
    }

    /** Returns the footer of the object.
     *
     * That is any HTML displayed after any object's contents.
     */
    public String footer(JsonObject object, String objectName) {
      return "";
    }

    /** Creates the HTML of the object.
     *
     * That is the caption, header, all its contents in order they were
     * added and then the footer. There should be no need to overload this
     * function, rather override the provided hooks above.
     */
    public String build(Response response, JsonObject object, String contextName) {
      StringBuilder sb = new StringBuilder();
      String name = elementName(contextName);
      sb.append(header(object, name));
      sb.append(header(object, name));
      for (Map.Entry<String,JsonElement> entry : object.entrySet()) {
        JsonElement e = entry.getValue();
        String elementContext = addToContext(contextName, entry.getKey());
        Builder builder = response.getBuilderFor(elementContext);
        if (builder == null)
          builder = defaultBuilder(e);
        sb.append(builder.build(response, e, elementContext));
      }
      sb.append(footer(object, elementName(contextName)));
      return sb.toString();
    }

    /** The original build method. Calls build with json object, if not an
     * object, displays an alert box with the JSON contents.
     */
    public String build(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonObject)
        return build(response, (JsonObject) element, contextName);
      return "<div class='alert alert-error'>Response element "+contextName+" expected to be JsonObject. Automatic display not available</div><pre>"+element.toString()+"</pre>";
    }
  }

  // ---------------------------------------------------------------------------
  // Array builder
  // ---------------------------------------------------------------------------

  /** Builds the HTML for an array. Arrays generally go to a table. Is similar
   * to the object, but rather than a horizontal dl generally displays as
   * a table.
   *
   * Can produce a header of the table and has hooks for rows.
   */
  public static class ArrayBuilder extends Builder {

    /** Caption of the table.
     */
    public String caption(JsonArray array, String name) {
      return "<h4>"+name+"</h4>";
    }

    /** Header of the table. Produces header off the first element if it is
     * object, or a single column header named value if it is a primitive. Also
     * includes the table tag.
     */
    public String header(JsonArray array) {
      StringBuilder sb = new StringBuilder();
      sb.append("<table class='table table-striped table-bordered'>");
      if (array.get(0) instanceof JsonObject) {
        sb.append("<tr>");
        for (Map.Entry<String,JsonElement> entry : ((JsonObject)array.get(0)).entrySet())
          sb.append("<th>"+JSON2HTML(entry.getKey()+"</th>"));
        sb.append("</tr>");
      } else {
        return "<tr><th>Value</th></tr>";
      }
      return sb.toString();
    }

    /** Footer of the table, the end of table tag.
     */
    public String footer(JsonArray array) {
      return "</table>";
    }

    /** Default builders for the table. It is either a table row builder if the
     * row is an object, or a row single column builder if it is a primitive
     * or another array.
     */
    @Override public Builder defaultBuilder(JsonElement element) {
      if (element instanceof JsonObject)
        return ARRAY_ROW_BUILDER;
      else
        return ARRAY_ROW_SINGLECOL_BUILDER;
    }

    /** Builds the array. Creates the caption, header, all the rows and the
     * footer or determines that the array is empty.
     */
    public String build(Response response, JsonArray array, String contextName) {
      StringBuilder sb = new StringBuilder();
      sb.append(caption(array, elementName(contextName)));
      if (array.size() == 0) {
        sb.append("<div class='alert alert-info'>empty array</div>");
      } else {
        sb.append(header(array));
        for (JsonElement e : array) {
          Builder builder = response.getBuilderFor(contextName+"_ROW");
          if (builder == null)
            builder = defaultBuilder(e);
          sb.append(builder.build(response, e, contextName));
        }
        sb.append(footer(array));
      }
      return sb.toString();
    }

    /** Calls the build method with array. If not an array, displays an alert
     * with the JSON contents of the element.
     */
    public String build(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonArray)
        return build(response, (JsonArray)element, contextName);
      return "<div class='alert alert-error'>Response element "+contextName+" expected to be JsonArray. Automatic display not available</div><pre>"+element.toString()+"</pre>";
    }

  }

  // ---------------------------------------------------------------------------
  // ElementBuilder
  // ---------------------------------------------------------------------------

  /** A basic element builder.
   *
   * Elements are displayed as their string values, everything else as their
   * JSON values.
   */
  public static class ElementBuilder extends Builder {

    public String elementToString(JsonElement element) {
      return element.getAsString();
    }

    public String objectToString(JsonObject object) {
      return object.toString();
    }

    public String arrayToString(JsonArray array) {
      return array.toString();
    }

    /** Displays the element in the horizontal dl layout. Override this method
     * to change the layout.
     */
    public String build(String elementContents, String elementName) {
      return "<dl class='dl-horizontal'><dt>"+elementName+"</dt><dd>"+elementContents+"</dd></dl>";
    }

    /** Based of the element type determines its string value and then calls
     * the string build version.
     */
    @Override public String build(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonArray)
        return build(arrayToString((JsonArray) element), elementName(contextName));
      else if (element instanceof JsonObject)
        return build(objectToString((JsonObject) element), elementName(contextName));
      else
        return build(elementToString(element), elementName(contextName));
    }
  }

  // ---------------------------------------------------------------------------
  // ArrayRowBuilder
  // ---------------------------------------------------------------------------

  /** A row in the array table.
   *
   * Is an object builder with no caption and header & footer being the
   * table row tags. Default builder is array row element (td).
   */
  public static class ArrayRowBuilder extends ObjectBuilder {
    public String caption(JsonObject object, String objectName) {
      return "";
    }

    public String header(JsonObject object, String objectName) {
      return "<tr>";
    }

    public String footer(JsonObject object, String objectName) {
      return "</tr>";
    }

    @Override public Builder defaultBuilder(JsonElement element) {
      return ARRAY_ROW_ELEMENT_BUILDER;
    }

  }

  // ---------------------------------------------------------------------------
  // ArrayRowElementBuilder
  // ---------------------------------------------------------------------------

  /** Default array row element.
   *
   * A simple element builder than encapsulates into a td.
   */
  public static class ArrayRowElementBuilder extends ElementBuilder {
    public String build(String elementContents, String elementName) {
      return "<td>"+elementContents+"</td>";
    }
  }

  // ---------------------------------------------------------------------------
  // ArrayRowSingleColBuilder
  // ---------------------------------------------------------------------------

  /** Array row for primitives.
   *
   * A row with single td element.
   */
  public static class ArrayRowSingleColBuilder extends ElementBuilder {
    public String build(String elementContents, String elementName) {
      return "<tr><td>"+elementContents+"</td></tr>";
    }
  }



}
