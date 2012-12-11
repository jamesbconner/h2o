
package water.api;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import water.NanoHTTPD;
import water.web.RString;

/** Base class of all requests that determines how the request handles its
 * arguments. While the code might in theory go to the Request class itself,
 * this is a simple mechanism how to make sure that the code is more modular
 * and more readable.
 *
 * Each request has its own arguments and its serve method can only access the
 * properly parsed arguments.
 *
 * @author peta
 */
public class BaseRequest {

  public static final String JSON_ERROR = "error";


  // ===========================================================================
  // Request dispatch
  // ===========================================================================


  // ---------------------------------------------------------------------------

  /** Serves the request returning the response object using the appropriate
   * request type.
   *
   * help and wiki requests are handled completely separately and their
   * arguments are discarded completely. See their respective methods.
   *
   * For json and www requests their argument are first checked. If the
   * arguments are correct, the serve method is called and the response object
   * is returned. In www mode the response object is interpreted by the HTML
   * engine and then returned to the user as web page.
   *
   * In www mode, if some of the arguments are not matched properly, the
   * automatic query build process is initiated and the query is returned
   * instead of the simple error.
   *
   * If in www mode and no arguments are present, the query is provided with no
   * errors.
   */
  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    String result = null; // string to which the result will be stored
    switch (type) {
      case wiki:
        result = serveWiki();
        break;
      case help:
        result = serveHelp();
        break;
      case json:
        result = checkArguments(args, type);
        if (result != null)
          return result;

      case www:
        result = checkArguments(args, type);

    }
  }




  /** Serves the help of the request page.
   *
   */
  private String serveHelp() {
    return "NOT IMPLEMENTED YET";
  }

  /** Serves the wiki of the request page.
   *
   */
  private String serveWiki() {

    return "NOT IMPLEMENTED YET";
  }



  // ===========================================================================
  // Helpers
  // ===========================================================================



}
