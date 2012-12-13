
package water.api;

import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Properties;
import water.web.RString;

/**
 *
 * @author peta
 */
public class RequestQueries extends RequestArguments {


  protected final JsonObject checkSingleArgument(Properties args, String argName) {
    _originalArguments.set(new Properties());
    _checkedArguments.set(new HashMap());
    _disabled.set(new Properties());
    JsonObject result = new JsonObject();
    for (Argument arg: _arguments) {
      if (arg._name.equals(argName)) {
        try {
          arg.check(args.getProperty(arg._name,""));
          result.addProperty(JSON_STATUS, JSON_STATUS_DONE);
          return result;
        } catch (IllegalArgumentException e) {
          result.addProperty(JSON_ERROR, "Argument "+arg._name+" error: "+e.getMessage());
          return result;
        }
      }
    }
    result.addProperty(JSON_ERROR,"Argument "+argName+" not defined for request "+getClass().getSimpleName());
    return result;
  }


  /** Checks the given arguments.
   *
   * When first argument is found wrong, generates the json error and returns the
   * result to be returned if any problems were found. Otherwise returns
   *
   * @param args
   * @param type
   * @return
   */
  protected final String checkArguments(Properties args, RequestType type) {
    _originalArguments.set(new Properties());
    _checkedArguments.set(new HashMap());
    _disabled.set(new Properties());
    if (type == RequestType.query)
      return buildQuery(args);
    for (Argument arg: _arguments) {
      try {
        arg.check(args.getProperty(arg._name,""));
      } catch (IllegalArgumentException e) {
        if (type == RequestType.json)
          return jsonError("Argument "+arg._name+" error: "+e.getMessage()).toString();
        else
          return buildQuery(args);
      }
    }
    return null;
  }

  private static final String _queryHtml =
            "<script type='text/javascript' src='queries.js'></script>"
          + "<h3>Request %REQ_NAME ( <a href='%REQ_NAME.help'>help</a> )</h3>"
          + "<p>Please specify the arguments for the request. If you have"
          + " already specified them, but they are wrong, or missing,"
          + " appropriate errors are displayed next to the form inputs.</p>"
          + "<p>Required fields are denoted by a red asterisk"
          + " <span style='color:#ff0000'>*</span>.</p>"
          + "<p></p>"
          + "<form id='query' class='form-horizontal'>"
          + "  <div class='control-group'><div class='controls'>"
          + "    <input type='submit' class='btn btn-primary' value='Send request' />"
          + "    <input type='reset' class='btn' value='Clear' />"
          + "  </div></div>"
          + "  %ARG_INPUT_HTML{"
          + "  <div id='inputQuery_error_%ARG_NAME' class='alert alert-error' style='%ERROR_STYLE'>%ERROR</div>"
          + "  <div id='inputQuery_controls_%ARG_NAME' class='control-group'>"
          + "    <label class='control-label' for='%ARG_NAME'>%ARG_ASTERISK %ARG_HELP</label>"
          + "    <div class='controls'>"
          + "      <input type='text' class='span5' disabled value='%NOTICE' style='%NOTICE_STYLE' />"
//          + "      <span id='inputQuery_notice_%ARG_NAME' class='label label-info' style='%NOTICE_STYLE'>%NOTICE</span>"
          + "      %ARG_INPUT_CONTROL"
          + "    </div>"
          + "  </div>"
          + "  <script type='text/javascript'>"
          + "  %SCRIPT"
          + "  </script>"
          + "  }"
          + "  <div class='control-group'><div class='controls'>"
          + "    <input type='submit' class='btn btn-primary' value='Send request' />"
          + "    <input type='reset' class='btn' value='Clear' />"
          + "  </div></div>"
          + "</form>"
          ;


  /** Returns the request query form produced from the given input arguments.
   */
  protected String buildQuery(Properties args) {
    RString query = new RString(_queryHtml);
    query.replace("REQ_NAME", this.getClass().getSimpleName());
    for (Argument arg: _arguments) {
      RString input = query.restartGroup("ARG_INPUT_HTML");
      input.replace("ARG_NAME",arg._name);
      input.replace("ARG_ASTERISK", DOM.color("*", arg._required ? "#ff0000" : "#ffffff"));
      input.replace("ARG_HELP", arg.help());
      try {
        arg.checkRequirements();
        if (arg.disabled() == null) {
          input.replace("NOTICE_STYLE","display:none");
          arg.check(args.getProperty(arg._name,""));
        } else {
          input.replace("NOTICE", arg.disabled());
        }
        input.replace("ERROR_STYLE","display:none");
      } catch (IllegalArgumentException e) {
        if (! args.isEmpty())
          input.replace("ERROR","Error: "+e.getMessage());
        else
          input.replace("ERROR_STYLE","display:none");
      }
      if (arg.disabled() ==  null)
        input.replace("ARG_INPUT_CONTROL", arg.query());

      input.append();
    }
    return query.toString();
  }

}
