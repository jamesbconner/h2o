
package water.api;

import water.web.RString;

/**
 *
 * @author peta
 */
public class HTTP404 extends Request {

  private final Str _error = new Str(JSON_ERROR,"Unknown error");

  public HTTP404() {
    _requestHelp = "Displays the HTTP 404 page with error specified in JSON"
            + " argument error.";
    _error._requestHelp = "Error description for the 404. Generally the URL not found.";
  }

  @Override public Response serve() {
    return Response.error(_error.value());
  }

  private static final String _html =
            "<h3>HTTP 404 - Not Found</h3>"
          + "<div class='alert alert-error'>%ERROR</div>"
          ;

  @Override protected String build(Response response) {
    RString str = new RString(_html);
    str.replace("ERROR", response.error());
    return str.toString();
  }

}
