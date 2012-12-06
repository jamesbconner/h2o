
package water.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author peta
 */
public class HTMLBuilder {

  public static final String RESPONSE_ROOT_ID = " ";

  public static final ObjectValue _responseFormatter = new ObjectValue();

//  public static final SimpleValue _simpleValueFormatter = new SimpleValue();
//  public static final TableValue _tableValueFormatter = new TableValue();
//  public static final TableCellValue _tableCellValue = new TableCellValue();

  public final JsonObject _json;

  protected JsonObject _current;

  public final HashMap<String, HTMLFormatter> _formatters;

  private int _depth;

  public HTMLBuilder(JsonObject json) {
    _formatters = new HashMap();
    _formatters.put(RESPONSE_ROOT_ID,_responseFormatter);
    _json = json;
    _current = json;
    _depth = 0;
  }

  /** In current context, returns the value of given JSON argument formatted
   * as string. It is assumed that you only ask for those arguments that are
   * present at the current level.
   */
  public String valueToString(String name) {
    assert (_current != null);
    assert (_current.has(name));
    return _current.get(name).getAsString();
  }


  /** Returns the array of all names of json elements on the current level. This
   * method is provided for simplicity of the code and is not 100% effective. On
   * the other hand, it is just a HTML page generation, so one can argue
   * performance is not the issue. Simplicity is.
   * 
   * @return
   */
  public String[] currentValues() {
    assert (_current != null);
    Set<Map.Entry<String,JsonElement>> entries = _current.entrySet();
    String[] result = new String[entries.size()];
    int i = 0;
    for (Map.Entry<String,JsonElement> e : entries) {
      result[i] = e.getKey();
    }
    return result;
  }




  public static String JSONNameToHTML(String name) {
    return name.substring(0,1).toUpperCase()+name.replace("_"," ").substring(1);
  }


  public static abstract class HTMLFormatter {
    public abstract void format(StringBuilder sb, String name, HTMLBuilder builder);
  }

  public static class ObjectValue extends HTMLFormatter {

    @Override public void format(StringBuilder sb, String name, HTMLBuilder builder) {



    }



  }


  /** A simple builder for a JSON element. */
  public static class SingleValue extends HTMLFormatter {
    public void format(StringBuilder sb, String name, HTMLBuilder builder) {
      DOM.dlHorizontal(sb,JSONNameToHTML(name),builder.valueToString(name));
    }
  }

  public static class TableValue extends HTMLFormatter {
    public void format(StringBuilder sb, String name, HTMLBuilder builder) {

    }
  }

  public static class TableCellValue extends HTMLFormatter {
    public void format(StringBuilder sb, String name, HTMLBuilder builder) {

    }
  }




}
