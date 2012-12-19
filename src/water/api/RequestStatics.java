
package water.api;

import com.google.gson.JsonObject;
import java.util.regex.Pattern;

/** All statics for the Request api.
 *
 * Especially the JSON property names should be defined here. Some helper
 * functions too.
 *
 * @author peta
 */
public class RequestStatics {

  public static final String JSON_RESPONSE = "response";
  public static final String JSON_ERROR = "error";
  public static final String JSON_REDIRECT = "redirect_request";
  public static final String JSON_REDIRECT_ARGS = "redirect_request_args";
  public static final String JSON_STATUS = "status";
  public static final String JSON_H2O = "h2o";
  public static final String JSON_H2O_NODE = "node";
  public static final String JSON_REQUEST_TIME = "time";
  public static final String JSON_PROGRESS = "progress";
  public static final String JSON_PROGRESS_TOTAL = "progress_total";


  public static final String JSON_KEY = "key";
  public static final String JSON_FAILED = "failed";
  public static final String JSON_SUCCEEDED = "succeeded";
  public static final String JSON_URL = "url";
  public static final String JSON_FILE = "file";
  public static final String JSON_VALUE = "value";
  public static final String JSON_VALUE_SIZE = "value_size";
  public static final String JSON_RF = "rf";
  public static final String JSON_DATA_KEY = "data_key";
  public static final String JSON_NUM_TREES = "ntree";
  public static final String JSON_DEPTH = "depth";
  public static final String JSON_SAMPLE = "sample";
  public static final String JSON_BIN_LIMIT = "bin_limit";
  public static final String JSON_GINI = "gini";
  public static final String JSON_SEED = "seed";
  public static final String JSON_PARALLEL = "parallel";
  public static final String JSON_MODEL_KEY = "model_key";
  public static final String JSON_CLASS = "class";
  public static final String JSON_IGNORE = "ignore";
  public static final String JSON_OOBEE = "oobee";
  public static final String JSON_FEATURES = "features";
  public static final String JSON_STRATIFY = "stratify";
  public static final String JSON_STRATA = "strata";
  public static final String JSON_WEIGHTS = "class_wt";
  public static final String JSON_FILTER = "filter";
  public static final String JSON_KEYS = "keys";
  public static final String JSON_LIMIT = "limit";
  public static final String JSON_COLUMNS = "columns";
  public static final String JSON_NO_CM = "no_cm";

  public static final String JSON_CLOUD_NAME = "cloud_name";
  public static final String JSON_NODE_NAME = "node_name";
  public static final String JSON_CLOUD_SIZE = "cloud_size";
  public static final String JSON_CONSENSUS = "consensus";
  public static final String JSON_LOCKED = "locked";
  public static final String JSON_NODES = "nodes";
  public static final String JSON_NODES_NAME = "name";
  public static final String JSON_NODES_NUM_CPUS = "num_cpus";
  public static final String JSON_NODES_FREE_MEM = "free_mem";
  public static final String JSON_NODES_TOT_MEM = "tot_mem";
  public static final String JSON_NODES_MAX_MEM = "max_mem";
  public static final String JSON_NODES_NUM_KEYS = "num_keys";
  public static final String JSON_NODES_VAL_SIZE = "val_size";
  public static final String JSON_NODES_FREE_DISK = "free_disk";
  public static final String JSON_NODES_MAX_DISK = "max_disk";
  public static final String JSON_NODES_CPU_UTIL = "cpu_util";
  public static final String JSON_NODES_CPU_LOAD_1 = "cpu_load_1";
  public static final String JSON_NODES_CPU_LOAD_5 = "cpu_load_5";
  public static final String JSON_NODES_CPU_LOAD_15 = "cpu_load_15";
  public static final String JSON_NODES_FJ_THREADS_HI = "fj_threads_hi";
  public static final String JSON_NODES_FJ_QUEUE_HI = "fj_queue_hi";
  public static final String JSON_NODES_FJ_THREADS_LO = "fj_threads_lo";
  public static final String JSON_NODES_FJ_QUEUE_LO = "fj_queue_lo";
  public static final String JSON_NODES_RPCS = "rpcs";
  public static final String JSON_NODES_TCPS_ACTIVE = "tcps_active";


  public String requestName() {
    return getClass().getSimpleName();
  }


  /** Request type.
   *
   * Requests can have multiple types. Basic types include the plain json type
   * in which the result is returned as a JSON object, a html type that acts as
   * the webpage, or the help type that displays the extended help for the
   * request.
   *
   * The wiki type is also added that displays the markup of the wiki that
   * should be used to document the request as per Matt's suggestion.
   *
   * NOTE the requests are distinguished by their suffixes. Please make the
   * suffix start with the dot character to avoid any problems with request
   * names.
   */
  public static enum RequestType {
    json(".json"), ///< json type request, a result is a JSON structure
    www(".html"), ///< webpage request
    help(".help"), ///< should display the help on the given request
    wiki(".wiki"), ///< displays the help for the given request in a markup for wiki
    query(".query"), ///< fDisplays the query for the argument in html mode
    ;
    /** Suffix of the request - extension of the URL.
     */
    public final String _suffix;

    RequestType(String suffix) {
      _suffix = suffix;
    }

    /** Returns the request type of a given URL. JSON request type is the default
     * type when the extension from the URL cannot be determined.
     */
    public static RequestType requestType(String requestUrl) {
      if (requestUrl.endsWith(www._suffix))
        return www;
      if (requestUrl.endsWith(help._suffix))
        return help;
      if (requestUrl.endsWith(wiki._suffix))
        return wiki;
      if (requestUrl.endsWith(query._suffix))
        return query;
      return json;
    }

    /** Returns the name of the request, that is the request url without the
     * request suffix.
     */
    public String requestName(String requestUrl) {
      String result = (requestUrl.endsWith(_suffix)) ? requestUrl.substring(0, requestUrl.length()-_suffix.length()) : requestUrl;
      if (result.charAt(0) == '/')
        return result.substring(1);
      return result;
    }
  }

  /** Returns the name of the JSON property pretty printed. That is spaces
   * instead of underscores and capital first letter.
   * @param name
   * @return
   */
  public static String JSON2HTML(String name) {
    return name.substring(0,1).toUpperCase()+name.replace("_"," ").substring(1);
  }


  private static Pattern _correctJsonName = Pattern.compile("^[_a-z][_a-z0-9]*$");

  /** Checks if the given JSON name is valid. A valid JSON name is a sequence of
   * small letters, numbers and underscores that does not start with number.
   */
  public static boolean checkJsonName(String name) {
    return _correctJsonName.matcher(name).find();
  }

  protected static JsonObject jsonError(String error) {
    JsonObject result = new JsonObject();
    result.addProperty(JSON_ERROR, error);
    return result;
  }

}
