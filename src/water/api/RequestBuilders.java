
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

  public static final Builder OBJECT_BUILDER;
  public static final Builder ARRAY_BUILDER;
  public static final Builder ARRAY_ROW_BUILDER;
  public static final Builder ELEMENT_BUILDER;
  public static final Builder ARRAY_ROW_ELEMENT_BUILDER;

  /** This is a response class for the JSON.
   *
   */
  public static final class Response {

    public static enum Status {
      done,
      poll,
      redirect
    }

    private final Status _status;
    private final JsonObject _response;

    private Response(Status status) {
      this(status, new JsonObject());
    }

    private Response(Status status, JsonObject response) {
      _status = status;
      _response = response;
    }

    public static Response done() {
      return new Response(Status.done);
    }

    public static Response done(JsonObject response) {
      return new Response(Status.done, response);
    }

    public void add(String property, JsonElement value) {
      _response.add(property,value);
    }

    public void addProperty(String property, String value) {
      _response.addProperty(property, value);
    }

    public void addProperty(String property, Character value) {
      _response.addProperty(property, value);
    }

    public void addProperty(String property, Number value) {
      _response.addProperty(property, value);
    }
    public void addProperty(String property, Boolean value) {
      _response.addProperty(property, value);
    }

    private HashMap<String,Builder> _builders = new HashMap();

    protected Builder getBuilderFor(String name) {
      return _builders.get(name);
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

  }

  // ---------------------------------------------------------------------------
  // ObjectBuilder
  // ---------------------------------------------------------------------------

  public class ObjectBuilder extends Builder {

    public String caption(JsonObject object, String objectName) {
      return "<h3>"+objectName+"</h3>";
    }

    public String header(JsonObject object, String objectName) {
      return "";
    }

    public String footer(JsonObject object, String objectName) {
      return "";
    }

    public String element(Response response, JsonElement element, String elementName, String elementContext, Builder elementBuilder) {
      return "<dl><dt>"+elementName+"</dt><dd>"+elementBuilder.format(response, element, elementContext)+"</dd></dl>";
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
        if (builder == null) {
          if (e instanceof JsonArray)
            builder = ARRAY_BUILDER;
          else if (e instanceof JsonObject)
            builder = OBJECT_BUILDER;
          else
            builder = ELEMENT_BUILDER;
        }
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

  public class ArrayBuilder extends Builder {

    public String caption(JsonArray array, String name) {
      return "<h3>"+name+"</h3>";
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

    public String row(Response response, JsonArray array, JsonElement element, String contextName) {
      Builder builder = response.getBuilderFor(contextName+"_ROW");
      if (builder == null) {
        if (element instanceof JsonObject)
          builder = ARRAY_ROW_BUILDER;
        else
          builder = ARRAY_ROW_ELEMENT_BUILDER;
      }
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

  public static class ElementBuilder extends Builder {

    public String elementToString(JsonElement element) {
      return element.getAsString();
    }

    public String objectToString(JsonObject object) {
      return object.toString();
    }

    public String arrayToString(JsonArray array) {
      return array.to
    }

    @Override public String format(Response response, JsonElement element, String contextName) {
      if (element instanceof JsonObject)
    }

  }




  protected String format(JsonObject response) {
    StringBuilder sb = new StringBuilder();
    sb.append(DOM.h3(getClass().getSimpleName()));
    sb.append(_format(response));
    return sb.toString();
  }

  protected String _format(JsonObject obj) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String,JsonElement> entry : obj.entrySet()) {
      JsonElement e = entry.getValue();
      if (e instanceof JsonObject) {
        sb.append(DOM.h3(JSON2HTML(entry.getKey())));
        sb.append(_format((JsonObject) e));
      } else if (e instanceof JsonArray) {
        sb.append(DOM.h3(JSON2HTML(entry.getKey())));
        sb.append(_format((JsonArray) e));
      } else {
        sb.append(DOM.dlHorizontal(JSON2HTML(entry.getKey()),_format(e)));
      }
    }
    return sb.toString();
  }

  protected String _tableHeader(JsonObject obj) {
    StringBuilder sb = new StringBuilder();
    sb.append("<tr>");
    for (Map.Entry<String,JsonElement> entry : obj.entrySet())
      sb.append(DOM.th(JSON2HTML(entry.getKey())));
    sb.append("</tr>");
    return sb.toString();
  }

  protected String _format(JsonArray ary) {
    if (ary.size()==0) {
      return DOM.p("empty");
    }
    StringBuilder sb = new StringBuilder();
    sb.append("<table class='table table-striped table-bordered'>");
    if (!(ary.get(0) instanceof JsonObject)) {
      sb.append("<tr>"+DOM.th("Value")+"</tr>");
      for (JsonElement e : ary) {
        sb.append("<tr>");
        sb.append(DOM.td(_format(e)));
        sb.append("</tr>");
      }
    } else {
      sb.append(_tableHeader((JsonObject)ary.get(0)));
      for (JsonElement e : ary) {
        sb.append("<tr>");
        for (Map.Entry<String,JsonElement> entry : ((JsonObject) e).entrySet())
          sb.append(DOM.td(_format(entry.getValue())));
        sb.append("</tr>");
      }
    }
    sb.append("</table>");
    return sb.toString();
  }

  protected String _format(JsonElement element) {
    return element.getAsString();
  }

}
