
package water.web;

import java.util.Properties;
import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class RFBuildQuery1 extends H2OPage {
  @Override public String[] requiredArguments() {
    return new String[] { "dataKey" };
  }

  static String html = "<p>We will be building a random forest from <b>%key</b>"
          + "<form class='form-horizontal' action='RFBuildQuery2'>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='dataKey'>Data</label>"
          + "    <div class='controls'>"
          + "      <input class='uneditable-input span5' type='text' id='dataKey' name='dataKey' value='%dataKey'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='class'>Class column</label>"
          + "    <div class='controls'>"
          + "      <select id='class' name='class'>"
          + "        %colClass{<option value='%colIdx'>%colName</option>}"
          + "      </select>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <div class='controls'>"
          + "      <button type='submit' class='btn btn-primary'>Next</button>"
          + "    </div>"
          + "  </div>"
          + "</form>"
          ;

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = new RString(html);
    result.replace("dataKey",args.getProperty("dataKey"));
    Value v = DKV.get(Key.make(args.getProperty("dataKey")));
    if (v == null)
      throw new PageError("Key not found!");
    if (!(v instanceof ValueArray))
      throw new PageError("Key is not a dataframe");
    ValueArray va = (ValueArray) v;
    for (int i = 0; i < va.num_cols(); ++i) {
      RString str = result.restartGroup("colClass");
      str.replace("colIdx",i);
      str.replace("colName",va.col_name(i) == null ? i : va.col_name(i));
      str.append();
    }
    return result.toString();
  }

}
