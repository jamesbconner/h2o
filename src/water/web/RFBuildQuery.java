
package water.web;

import java.util.Properties;

import water.ValueArray;

public class RFBuildQuery extends H2OPage {
  private static final String DATA_KEY  = RandomForestPage.DATA_KEY;
  private static final String MODEL_KEY = RandomForestPage.MODEL_KEY;
  private static final String CLASS_COL = RandomForestPage.CLASS_COL;

  public static final String NUM_TREE   = RandomForestPage.NUM_TREE;
  public static final String MAX_DEPTH  = RandomForestPage.MAX_DEPTH;
  public static final String SAMPLE     = RandomForestPage.SAMPLE;
  public static final String BIN_LIMIT  = RandomForestPage.BIN_LIMIT;
  public static final String GINI       = RandomForestPage.GINI;
  public static final String IGNORE_COL = RandomForestPage.IGNORE_COL;

  private String input(String clz, String type, String name, String placeholder) {
    RString res = new RString(
        "<input class='%clz' type='%type' name='%name' id='%name' placeholder='%placeholder' value=\"%%%name\">");
    res.replace("clz", clz);
    res.replace("type", type);
    res.replace("name", name);
    res.replace("placeholder", placeholder);
    return res.toString();
  }

  private String select(String name, String description, boolean multiple, String[] options) {
    StringBuilder res = new StringBuilder();
    res.append("<label for='").append(name).append("'>");
    res.append(description);
    res.append("</label>&nbsp;&nbsp;");
    res.append("<select ");
    if(multiple) res.append("multiple='true' ");
    res.append("name='").append(name).append("' ");
    res.append(">");
    for( String o : options ) {
      res.append("<option value='").append(o).append("'>");
      res.append(o);
      res.append("</option>");
    }
    res.append("</select>");
    return res.toString();
  }

  private RString html(ValueArray va) {
    if( va == null ) {
      return new RString(
          "Select a hex key to use as the learning set.<hr>" +
          "<form class='well form-inline' action='RFBuildQuery'>" +
          input("input-small span4", "text", DATA_KEY, "Hex key for learning") +
          "<button class='btn btn-primary' type='submit'>Update</button>" +
          "</form>");
    }
    String[] cols = va.col_names();

    return new RString(
        "Select the parameters for the random forest.<hr>" +
        "<form class='well form-inline' action='RF'>" +
        input("input-small span4", "text", DATA_KEY,  "Hex key for data") +"<br>"+
        select(CLASS_COL, "Column to learn", false, cols) +"<br>"+
        select(IGNORE_COL, "Columns to ignore", true, cols) +"<br>"+
        input("input-small span4", "text", MODEL_KEY, "Key for result model") +"<br>"+
        "  <button class='btn btn-primary' type='submit'>Build Random Forest</button>" +
        "</form>"
        );
  }

  @Override protected String serveImpl(Server server, Properties p, String sessionID) throws PageError {
    String key = p.getProperty(DATA_KEY, null);
    RString result = html(key == null ? null : ServletUtil.check_array(p, DATA_KEY));
    result.replace(DATA_KEY,  p.getProperty(DATA_KEY,  ""));
    result.replace(MODEL_KEY, p.getProperty(MODEL_KEY, ""));
    result.replace(CLASS_COL, p.getProperty(CLASS_COL, ""));
    return result.toString();
  }
}
