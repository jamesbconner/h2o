
package water.web;

import java.util.Properties;

public class RFViewQuery extends H2OPage {
  private static final String DATA_KEY  = RFView.DATA_KEY;
  private static final String MODEL_KEY = RFView.MODEL_KEY;
  private static final String CLASS_COL = RFView.CLASS_COL;

  private String input(String clz, String type, String name, String placeholder) {
    RString res = new RString(
        "<input class='%clz' type='%type' name='%name' id='%name' placeholder='%placeholder' value=\"%%%name\">");
    res.replace("clz", clz);
    res.replace("type", type);
    res.replace("name", name);
    res.replace("placeholder", placeholder);
    return res.toString();
  }

  private RString html() {
    return new RString(
        "Select the model & data and other arguments for the Random Forest View to look at:<br/>" +
        "<form class='well form-inline' action='RFView'>" +
        "  <button class='btn btn-primary' type='submit'>View</button>" +
        input("input-small span4", "text", DATA_KEY,  "Hex key for data") +
        input("input-small span4", "text", MODEL_KEY, "Hex key for model") +
        input("input-small span2", "text", CLASS_COL,    "Class Column") +
        "</form>"
        );
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = html();
    result.replace(DATA_KEY,  args.getProperty(DATA_KEY,  ""));
    result.replace(MODEL_KEY, args.getProperty(MODEL_KEY, ""));
    result.replace(CLASS_COL, args.getProperty(CLASS_COL, ""));
    return result.toString();
  }
}
