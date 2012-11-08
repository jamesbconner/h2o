
package water.web;

import java.util.Properties;

public class RFBuildQuery extends H2OPage {

  private RString html() {
    return new RString("" +
        "Select the model & data and other arguments for the Random Forest View to look at:<br/>" +
        "<form class='well form-inline' action='RFView'>" +
        "  <button class='btn btn-primary' type='submit'>View</button>" +
        "  <input class='input-small span4' type='text' name='dataKey' id='dataKey' placeholder='Hex key for data' value=\"%dataKey\">" +
        "  <input class='input-small span4' type='text' name='modelKey' id='modelKey' placeholder='Hex key for model' value=\"%modelKey\">" +
        "  <input class='input-small span2' type='text' name='class' id='class' placeholder='Class' value=\"%class\">" +
        "</form>"
        );
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = html();
    result.replace("dataKey",args.getProperty("dataKey",""));
    result.replace("modelKey",args.getProperty("modelKey",""));
    result.replace("class",args.getProperty("class",""));
    return result.toString();
  }

}
