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
public class RFViewQuery1 extends H2OPage {

  @Override public String[] requiredArguments() {
    return new String[] { "dataKey", "modelKey", "class" };
  }

  final String html = "Select the model & data and other arguments for the Random Forest View to look at:<br/>"
          + "<script type='text/javascript'>\n"
          + "function getWeightsString() {\n"
          + "    var str = \"\"\n"
          + "    for (var i = 0; i < %numClasses; ++i) {\n"
          + "      var name = document.getElementById('n'+i).value\n"
          + "      var weight = document.getElementById('w'+i).value\n"
          + "      if (weight == \"\")\n"
          + "        continue\n"
          + "      if (str == \"\")\n"
          + "        str = name+'='+weight\n"
          + "      else\n"
          + "        str = str+','+name+'='+weight\n"
          + "    }\n"
          + "    return str\n"
          + "}\n"       
          + "function sendForm() {\n"
          + "  document.getElementById('classWt').value = getWeightsString()\n"
          + "  document.forms['rfbuild'].submit()\n"
          + "}\n"
          + "</script>"
          + "<form class='form-horizontal' action='RFView' id='rfbuild'>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='Key'>Data</label>"
          + "    <div class='controls'>"
          + "      <input class='uneditable-input span5' type='text' id='dataKey' name='dataKey' value='%dataKey'>"
          + "      <input style='display:none' type='text' id='classWt' name='classWt'>"
          + "      <input style='display:none' type='text' id='clearCM' name='clearCM' value='1'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for=''>Class column</label>"
          + "    <div class='controls'>"
          + "      <input class='uneditable-input span5' type='text'  value='%classColName'>"
          + "      <input style='display:none' type='text' id='class' name='class' value='%classColIdx'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='modelKey'>Model</label>"
          + "    <div class='controls'>"
          + "      <input class='uneditable-input span5' type='text' id='modelKey' name='modelKey' value='%modelKey'placeholder='model key (default model)'>"
          + "    </div>"
          + "  </div>"
          + "</form>"
          + "<div class='form-horizontal'>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for=''>Class weights</label>"
          + "    <div class='controls'>"
          + "      %classWeight{"
          + "      <div class='input-append'>"
          + "        <input class='span1' id='w%wi' type='text' placeholder='(default 1)'><span class='add-on'>%className</span>"
          + "          <input style='display:none' id='n%wi' type='text' value='%className'>"
          + "      </div><br/><br/>}"
          + "    <button class='btn btn-primary' onclick='sendForm()' >Calculate RF</button>"
          + "    </div>"
          + "  </div>"
          + "</div>"
          ;

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = new RString(html);
    result.replace("dataKey",args.getProperty("dataKey",""));
    result.replace("modelKey",args.getProperty("modelKey",""));
    result.replace("classColIdx",args.getProperty("class",""));
    Value v = DKV.get(Key.make(args.getProperty("dataKey")));
    if (v == null)
      throw new PageError("Key not found!");
    if (!(v instanceof ValueArray))
      throw new PageError("Key is not a dataframe");
    ValueArray va = (ValueArray) v;
    int classCol = getAsNumber(args, "class", va.num_cols()-1);
    result.replace("classColName",va.col_name(classCol));
    String[] classes = RandomForestPage.determineColumnClassNames(va, classCol, RandomForestPage.MAX_CLASSES);
    result.replace("numClasses",classes.length);
    for (int i = 0; i < classes.length; ++i) {
      RString str = result.restartGroup("classWeight");
      str.replace("wi",i);
      str.replace("className",classes[i]);
      str.append();
    }
    return result.toString();
  }

}
