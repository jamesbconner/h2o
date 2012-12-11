
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Map;

/** My plan is to add formatters here, but since they appear not to be needed,
 * it has just a simple stuff in.
 *
 * It just has a stuff of simple builders that walk through the JSON response
 * and format the stuff into basic html. Understands simplest form of tables,
 * objects and elements.
 *
 * @author peta
 */
public class RequestFormatters extends RequestQueries {

  public static String JSON2HTML(String name) {
    return name.substring(0,1).toUpperCase()+name.replace("_"," ").substring(1);
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
    StringBuilder sb = new StringBuilder();
    sb.append("<table class='table table-striped table-bordered'>");
    sb.append(_tableHeader((JsonObject)ary.get(0)));
    for (JsonElement e : ary) {
      sb.append("<tr>");
      for (Map.Entry<String,JsonElement> entry : ((JsonObject) e).entrySet())
        sb.append(DOM.td(_format(entry.getValue())));
      sb.append("</tr>");
    }
    sb.append("</table>");
    return sb.toString();
  }

  protected String _format(JsonElement element) {
    return element.getAsString();
  }

}
