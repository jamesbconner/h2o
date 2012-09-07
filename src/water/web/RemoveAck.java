package water.web;

import java.util.Properties;
import water.Key;

/**
 *
 * @author peta
 */
public class RemoveAck extends H2OPage {

  @Override protected String serve_impl(Properties args) {
    String keys = args.getProperty("Key");
    Key key = null;
    try {
      key = Key.make(keys);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return H2OPage.error("Not a valid key: "+ keys);
    }
    if (!key.user_allowed())
      return error("Not a user key: "+ key.toString());
    RString response = new RString(html);
    response.replace("key",key.toString());
    response.replace("keyHref",encode(key._kb));
    return response.toString();
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }


  static final String html =
    "<div class='alert alert-error'>Are you sure you want to delete key <strong>%key</strong>?<br/>"
    + "There is no way back!"
    + "</div>"
    + "<div style='text-align:center'>"
    + "<a href='StoreView'><button class='btn btn-primary'>No, back to node</button></a>"
    + "&nbsp;&nbsp;&nbsp;"
    + "<a href='Remove?Key=%keyHref'><button class='btn btn-danger'>Yes!</button></a>"
    + "</div>"
    ;
}
