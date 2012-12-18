
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;

/** My plan is to add formatters here, but since they appear not to be needed,
 * it has just a simple stuff in.
 *
 * It just has a stuff of simple builders that walk through the JSON response
 * and format the stuff into basic html. Understands simplest form of tables,
 * objects and elements.
 *
 *
 *
 *
 * _ROW append for array rows
 *
 * @author peta
 */
public class RequestBuilders extends RequestQueries {

  public static final Builder OBJECT_BUILDER = new ObjectBuilder();
  public static final Builder ARRAY_BUILDER = new ArrayBuilder();
  public static final Builder ARRAY_ROW_BUILDER = new ArrayRowBuilder();
  public static final Builder ELEMENT_BUILDER = new ElementBuilder();
  public static final Builder ARRAY_ROW_ELEMENT_BUILDER = new ArrayRowElementBuilder();

  /** This is a response class for the JSON.
   *
   */
  public static final class Response {

    public static enum Status {
      done,
      poll,
      redirect,
      error
    }

    private final Status _status;
    private final JsonObject _response;

    private Response(Status status, JsonObject response) {
      _status = status;
      _response = response;
    }

    public static Response error(String message) {
      JsonObject obj = new JsonObject();
      obj.addProperty(JSON_ERROR,message);
      return new Response(Status.error,obj);
    }

    public static Response done(JsonObject response) {
      return new Response(Status.done, response);
    }

    private HashMap<String,Builder> _builders = new HashMap();

    protected Builder getBuilderFor(String name) {
      return _builders.get(name);
    }

    public JsonObject toJson() {
      return _response;
    }

    public String error() {
      if (_status != Status.error)
        return null;
      return _response.get(JSON_ERROR).getAsString();
    }

  }


  // ---------------------------------------------------------------------------
  // Builder
  // ---------------------------------------------------------------------------

  public static abstract class Builder {
    public abstract String format(Response response, JsonElement element, String contextName);

    public static String addToContext(String oldContext, String name) {
      if (oldContext.isEmpty())
        return name;
      return oldContext+"."+name;
    }

    public static String elementName(String context) {
     int idx = context.lastIndexOf(".");
     return context.substring(idx+1);
    }

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

  public static class ObjectBuilder extends Builder {

    public String caption(JsonObject object, String objectName) {
      return objectName.isEmpty() ? "" : "<h4>"+objectName+"</h4>";
    }

    public String header(JsonObject object, String objectName) {
      return "";
    }

    public String footer(JsonObject object, String objectName) {
      return "";
    }

    public String element(Response response, JsonElement element, String elementName, String elementContext, Builder elementBuilder) {
      return elementBuilder.format(response,element,elementContext);
    }

    public String format(Response response, JsonObject object, String contextName) {
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
        sb.append(element(response, e, entry.getKey(), elementContext, builder));
      }
      sb.append(footer(object, elementName(contextName)));
      return sb.toString();
    }

    public String format(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonObject)
        return format(response, (JsonObject) element, contextName);
      return "<div class='alert alert-error'>Response element "+contextName+" expected to be JsonObject. Automatic display not available</div><pre>"+element.toString()+"</pre>";
    }
  }

  // ---------------------------------------------------------------------------
  // Array builder
  // ---------------------------------------------------------------------------

  public static class ArrayBuilder extends Builder {

    public String caption(JsonArray array, String name) {
      return "<h4>"+name+"</h4>";
    }

    public String header(JsonArray array) {
      StringBuilder sb = new StringBuilder();
      sb.append("<table class='table table-striped table-bordered'>");
      if (array.get(0) instanceof JsonObject) {
        sb.append("<tr>");
        for (Map.Entry<String,JsonElement> entry : ((JsonObject)array.get(0)).entrySet())
          sb.append(DOM.th(JSON2HTML(entry.getKey())));
        sb.append("</tr>");
      } else {
        return "<tr><th>Value</th></tr>";
      }
      return sb.toString();
    }

    public String footer(JsonArray array) {
      return "";
    }

    @Override public Builder defaultBuilder(JsonElement element) {
      if (element instanceof JsonObject)
        return ARRAY_ROW_BUILDER;
      else
        return ARRAY_ROW_ELEMENT_BUILDER;
    }

    public String row(Response response, JsonArray array, JsonElement element, String contextName) {
      Builder builder = response.getBuilderFor(contextName+"_ROW");
      if (builder == null)
        builder = defaultBuilder(element);
      return builder.format(response, element, contextName);
    }

    public String format(Response response, JsonArray array, String contextName) {
      StringBuilder sb = new StringBuilder();
      sb.append(caption(array, elementName(contextName)));
      if (array.size() == 0) {
        sb.append("<div class='alert alert-info'>empty array</div>");
      } else {
        sb.append(header(array));
        for (JsonElement e : array) {
          sb.append(row(response, array, e, contextName));
        }
        sb.append(footer(array));
      }
      return sb.toString();
    }

    public String format(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonArray)
        return format(response, (JsonArray)element, contextName);
      return "<div class='alert alert-error'>Response element "+contextName+" expected to be JsonArray. Automatic display not available</div><pre>"+element.toString()+"</pre>";
    }

  }

  // ---------------------------------------------------------------------------
  // ElementBuilder
  // ---------------------------------------------------------------------------

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

    public String build(String elementContents, String elementName) {
      return "<dl class='dl-horizontal'><dt>"+elementName+"</dt><dd>"+elementContents+"</dd></dl>";
    }

    @Override public String format(Response response, JsonElement element, String contextName) {
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

    public String element(Response response, JsonElement element, String elementName, String elementContext, Builder elementBuilder) {
      return elementBuilder.format(response,element,elementContext);
    }

    @Override public Builder defaultBuilder(JsonElement element) {
      return ARRAY_ROW_ELEMENT_BUILDER;
    }

  }

  // ---------------------------------------------------------------------------
  // ArrayRowElementBuilder
  // ---------------------------------------------------------------------------

  public static class ArrayRowElementBuilder extends ElementBuilder {
    public String build(String elementContents, String elementName) {
      return "<td>"+elementContents+"</td>";
    }
  }

  protected String build(Response response) {

    StringBuilder sb = new StringBuilder();
    sb.append("<h3>"+getClass().getSimpleName()+" response:</h3>");
    Builder builder = response.getBuilderFor("");
    if (builder == null)
      builder = OBJECT_BUILDER;
    sb.append(builder.format(response,response._response,""));
    return sb.toString();
  }

}
