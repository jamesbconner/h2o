
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Handles the almost automatic transcription of JSON responses to HTML code.
 *
 * A HTML builder is created for a HTML response. The idea of the builder is
 * very simple. Each json element (nested elements are separated by dots) can
 * be assigned its own builder, which is a simple class that builds the
 * corresponding HTML code.
 *
 * The builders can either be the automatic ones which reasonably well present
 * most of the data, or can be customized by the user. Then the HTML is built
 * by recursively calling the builders.
 *
 * ARRAY_ELEMENT is used as a name for array elements (generally table rows).
 *
 * @author peta
 */
public class HTMLBuilder {

  public static final String ARRAY_ELEMENT = ".";
  public static final String RESPONSE_ROOT = "";

  /** Name of the request calling */
  public final String _requestName;

  /** The JSON response root. */
  public final JsonObject _response;

  /** Current level of the response while building. That is the parent of the
   * element currently being built.
   */
  protected JsonObject _current;

  /** Depth of the building process, starts with 0 for the root element. Only
   * elements containing other elements have depths.
   */
  protected int _currentDepth;

  /** Name of the current element in the build process. */
  protected String _currentName;

  /** String builder used to build the HTML code. */
  protected StringBuilder _sb;


  /** Hashmap of the builders. Each builder is in its context name */
  public final HashMap<String, Builder> _builders = new HashMap();


  public HTMLBuilder(Request request, JsonObject json) {
    _currentDepth = 0;
    _current = null;
    _currentName = "";
    _response = json;
    _requestName = request._href;
  }

  public String build() {
    _sb = new StringBuilder();
    _responseBuilder.getBuilder(RESPONSE_ROOT).build(RESPONSE_ROOT,_response);
    return _sb.toString();
  }

  public void setBuilder(String builderName, Builder builder) {
    _builders.put(builderName, builder);
  }

  public static String JSON2HTML(String name) {
    return name.substring(0,1).toUpperCase()+name.replace("_"," ").substring(1);
  }






  private Builder _tableRowBuilder = new TableRow();
  private Builder _tableCellBuilder = new TableCell();
  private Builder _tableBuilder = new Table();
  private Builder _nestedValueBuilder = new NestedObject();
  private Builder _singleValueBuilder = new SingleValue();
  private Builder _responseBuilder = new Response();

  public abstract class Builder {

    /** Override this method to specify how the given element is transformed to
     * the HTML code.
     *
     * @param name
     * @param value
     */
    public abstract void build(String name, JsonElement value);


    public final String getContextName(String name) {
      if (_currentName.isEmpty())
        return name;
      if (name.equals(ARRAY_ELEMENT))
        return _currentName;
      return _currentName+"."+name;
    }

    public Builder getBuilder(String name) {
      String ctxName = getContextName(name);
      if (_builders.containsKey(ctxName))
        return _builders.get(getContextName(name));
      if (_currentDepth == 2)
        return _tableRowBuilder;
      if (_currentDepth >= 3)
        return _tableCellBuilder;
      if (_currentDepth == 0)
        return _responseBuilder;
      JsonElement el = _current.get(name);
      if (el instanceof JsonArray)
        return _tableBuilder;
      if (el instanceof JsonObject)
        return _nestedValueBuilder;
      return _singleValueBuilder;
    }


  }

  public class NestedBuilder extends Builder {

    public void beforeBuilding(String name, JsonElement value) {
      // pass
    }

    public void afterBuilding(String name, JsonElement value) {
      // pass
    }

    @Override public void build(String name, JsonElement value) {
      if (!(value instanceof JsonObject)) {
        DOM.warning(_sb,"JSON element "+getContextName(name)+" is not an expected object. Value: "+value.toString());
      } else {
        JsonObject objValue = (JsonObject) value;
        JsonObject oldCurrent = _current;
        String oldCurrentName = _currentName;
        // update the current context in the builder
        _current = objValue;
        ++_currentDepth;
        _currentName = getContextName(name);
        // call before building event
        beforeBuilding(name, value);
        // build the nested elements
        for (Map.Entry<String,JsonElement> e : objValue.entrySet()) {
          Builder b = getBuilder(e.getKey());
          if (b != null)
            b.build(e.getKey(), e.getValue());
        }
        // call the after building elements
        afterBuilding(name, value);
        // put back the old context
        _current = oldCurrent;
        _currentName = oldCurrentName;
        --_currentDepth;
      }
    }
  }

  public class ArrayBuilder extends NestedBuilder {
    @Override public void build(String name, JsonElement value) {
      if (!(value instanceof JsonArray)) {
        DOM.warning(_sb,"JSON element "+getContextName(name)+" is not an expected array. Value: "+value.toString());
      } else {
        JsonArray aryValue = (JsonArray) value;
        String oldCurrentName = _currentName;
        // do not update the current container, only the name
        ++_currentDepth;
        _currentName = getContextName(name);
        // call before building event
        beforeBuilding(name, value);
        // build the nested elements
        for (int i = 0; i < aryValue.size(); ++i ) {
          Builder b = getBuilder(ARRAY_ELEMENT);
          if (b != null)
            b.build(ARRAY_ELEMENT, aryValue.get(i));
        }
        // call the after building elements
        afterBuilding(name, value);
        // put back the old context
        _currentName = oldCurrentName;
        --_currentDepth;
      }
    }
  }


  /** A simple builder that just converts the value to the string and
   * displays it in a horizontal dl element.
   */
  public class SingleValue extends Builder {

    @Override public void build(String name, JsonElement value) {
      DOM.dlHorizontal(_sb,JSON2HTML(name),value.getAsString());
    }
  }

  /** Builder that formats the given value to a table cell. Only the value, no
   * name is reported.
   */
  public class TableCell extends Builder {

    @Override public void build(String name, JsonElement value) {
      DOM.td(_sb, value.getAsString());
    }
  }

  public class TableRow extends NestedBuilder {
    @Override public void beforeBuilding(String name, JsonElement value) {
      _sb.append("<tr>");
    }

    @Override public void afterBuilding(String name, JsonElement value) {
      _sb.append("</tr>");
    }
  }

  public class Table extends ArrayBuilder {

    protected void buildHeader(JsonObject from) {
      _sb.append("<tr>");
      for (Map.Entry<String, JsonElement> e : from.entrySet()) {
        _sb.append("<th>");
        _sb.append(JSON2HTML(e.getKey()));
        _sb.append("</th>");
      }
      _sb.append("</tr>");
    }

    @Override public void beforeBuilding(String name, JsonElement value) {
      _sb.append("<h3>");
      _sb.append(JSON2HTML(name));
      _sb.append("</h3>");
      JsonArray ary = (JsonArray) value;
      if (ary.size() == 0) {
        DOM.info(_sb,"empty");
      } else {
        _sb.append("<table class='table table-striped table-bordered'>");
        JsonElement first = ary.get(0);
        if (first instanceof JsonObject)
          buildHeader((JsonObject)first);
      }
    }

    @Override public void afterBuilding(String name, JsonElement value) {
      JsonArray ary = (JsonArray) value;
      if (ary.size() != 0)
        _sb.append("</table>");
    }
  }

  public class NestedObject extends NestedBuilder {
    @Override public void beforeBuilding(String name, JsonElement value) {
      _sb.append("<h3>");
      _sb.append(name);
      _sb.append("</h3>");
    }
  }

  public class Response extends NestedBuilder {
    @Override public void beforeBuilding(String name, JsonElement value) {
      _sb.append("<h2>");
      _sb.append(_requestName);
      _sb.append("</h2>");
    }
  }

  public abstract class LinkTableCellBuilder extends TableCell {

    protected abstract String makeHref(JsonElement value);

    @Override public void build(String name, JsonElement value) {
      DOM.td(_sb, DOM.a(value.getAsString(),makeHref(value)));
    }

  }

}
