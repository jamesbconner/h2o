
package water.api;

import java.util.Properties;
import water.web.RString;

/**
 *
 * @author peta
 */
public class RequestQueries extends RequestArguments {

  /** Overwrite this method to be able to change / disable values of other
   * arguments on certain argument changes.
   *
   * This is done for both query checking and request checking.
   */
  protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {

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
    // reset all arguments
    for (Argument arg: _arguments)
      arg.reset();
    // return query if in query mode
    if (type == RequestType.query)
      return buildQuery(args,type);
    // check the arguments now
    for (Argument arg: _arguments) {
      if (!arg.disabled()) {
        try {
          arg.check(args.getProperty(arg._name,""));
          queryArgumentValueSet(arg, args);
        } catch (IllegalArgumentException e) {
          if (type == RequestType.json)
            return jsonError("Argument "+arg._name+" error: "+e.getMessage()).toString();
          else
            return buildQuery(args,type);
        }
      }
    }
    return null;
  }

  private static final String _queryHtml =
            "<h3>Request %REQ_NAME ( <a href='%REQ_NAME.help'>help</a> )</h3>"
          + "<p>Please specify the arguments for the request. If you have"
          + " already specified them, but they are wrong, or missing,"
          + " appropriate errors are displayed next to the form inputs.</p>"
          + "<p>Required fields are denoted by a red asterisk"
          + " <span style='color:#ff0000'>*</span>.</p>"
          + "<p></p>"
          + "  <dl class='dl-horizontal'><dt></dt><dd>"
          + "    <button class='btn btn-primary' onclick='query_submit()'>Submit</button>"
          + "    <button class='btn btn-info' onclick='query_refresh()'>Refresh</button>"
          + "    <button class='btn' onclick='query_reset()'>Reset</button>"
          + "  </dd></dl>"
          + "    %QUERY"
          + "  <dl class='dl-horizontal'><dt></dt><dd>"
          + "    <button class='btn btn-primary' onclick='query_submit()'>Submit</button>"
          + "    <button class='btn btn-info' onclick='query_refresh()'>Refresh</button>"
          + "    <button class='btn' onclick='query_reset()'>Reset</button>"
          + "  </dd></dl>"
          + "  <script type='text/javascript'>"
          + "    %SCRIPT"
          + "  </script>"
          ;

  private static final String _queryJs =
            "function query_refresh() {\n"
          + "  query_submit('.query');\n"
          + "}\n"
          + "function query_submit(requestType, specArg, specValue) {\n"
          + "  if (typeof(requestType) === 'undefined')\n"
          + "    requestType='.html';\n"
          + "  var request = {};\n"
          + "  %REQUEST_ELEMENT{\n"
          + "    request.%ELEMENT_NAME = query_value_%ELEMENT_NAME();\n"
          + "  }\n"
          + "  if (typeof(specArg) !== 'undefined')\n"
          + "    request[specArg] = specValue;\n"
          + "  var location = '%REQUEST_NAME'+requestType+'?'+$.param(request);\n"
          + "  window.location = location;\n"
          + "}\n"
          + "function query_reset() {\n"
          + "  window.location='%REQUEST_NAME.query';\n"
          + "}\n"
          + "%ELEMENT_VALUE{ %BODY\n }"
          + "%ELEMENT_ADDONS{ %BODY\n }"
          + "%ELEMENT_ONCHANGE{ %BODY\n }"
          ;


  /** Returns the request query form produced from the given input arguments.
   */
  protected String buildQuery(Properties args, RequestType type) {
    if (args.isEmpty())
      type = RequestType.query;
    RString result = new RString(_queryHtml);
    result.replace("REQ_NAME", this.getClass().getSimpleName());
    StringBuilder query = new StringBuilder();
    query.append("<form onsubmit='return false;'>");
    RString script = new RString(_queryJs);
    script.replace("REQUEST_NAME", getClass().getSimpleName());
    for (Argument arg: _arguments) {
      try {
        arg.check(args.getProperty(arg._name,""));
        queryArgumentValueSet(arg, args);
      } catch (IllegalArgumentException e) {
        // in query mode only display error for arguments present
        if ((type != RequestType.query) || !args.getProperty(arg._name,"").isEmpty())
          query.append("<div class='alert alert-error'>"+e.getMessage()+"</div>");
      }
      query.append(arg.query());
      if (!arg.disabled()) {
        RString x = script.restartGroup("REQUEST_ELEMENT");
        x.replace("ELEMENT_NAME",arg._name);
        x.append();
        x = script.restartGroup("ELEMENT_VALUE");
        x.replace("ELEMENT_NAME",arg._name);
        x.replace("BODY","function query_value_"+arg._name+"() { "+arg.jsValue()+"} ");
        x.append();
      }
      if (arg.refreshOnChange()) {
        RString x = script.restartGroup("ELEMENT_ONCHANGE");
        x.replace("BODY",arg.jsRefresh("query_refresh"));
        x.append();
      }
      RString x = script.restartGroup("ELEMENT_ADDONS");
      x.replace("BODY", arg.jsAddons());
      x.append();
    }
    query.append("</form>");
    result.replace("QUERY",query.toString());
    result.replace("SCRIPT",script.toString());
    return result.toString();
  }

}
