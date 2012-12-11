
package water.api;

import java.util.HashMap;
import java.util.Properties;
import water.web.RString;

/**
 *
 * @author peta
 */
public class RequestQueries extends RequestArguments {

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
            "<h3>Request %REQ_NAME</h3>"
          + "<p>Please specify the arguments for the request. If you have"
          + " already specified them, but they are wrong, or missing,"
          + " appropriate errors are displayed next to the form inputs.</p>"
          + "<p>Required fields are denoted by a red asterisk"
          + " <span style='color:#ffc0c0'>*</span></p>."
          + "<p><a href='%REQ_NAME.help'>Request help</a></p>"
          + "<form>"
          + "  <div class='control-group'><div class='controls'>"
          + "    <input type='submit' class='btn btn-primary' value='Send request' />"
          + "    <input type='reset' class='btn' value='Clear' />"
          + "  </div></div>"
          + "  %ARG_INPUT_HTML{"
          + "  %ERROR"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='%ARG_NAME'>%ARG_ASTERISK %ARG_HELP</label>"
          + "    <div class='controls'>"
          + "      %ARG_INPUT_CONTROL"
          + "    </div>"
          + "  </div>"
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
      input.replace("ARG_ASTERISK", DOM.color( arg._required ? "#ff0000" : "#ffffff", "*"));
      input.replace("ARG_HELP", arg.help());
      if (! args.isEmpty()) {
        try {
          arg.check(args.getProperty(arg._name,""));
        } catch (IllegalArgumentException e) {
          input.replace("ERROR","Error: "+e.getMessage());
        }
      }
      input.replace("ARG_INPUT_CONTROL", arg.query());
    }
    return query.toString();
  }

}
