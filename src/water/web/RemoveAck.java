package water.web;

import java.util.Properties;
import water.Key;

/**
 *
 * @author peta
 */
public class RemoveAck extends H2OPage {

  @Override protected String serve_impl(Properties args) {
    Object o = ServletUtil.check_key(args,"Key");
    if( o instanceof String ) return (String)o;
    Key key = (Key)o;
    RString response = new RString(html);
    response.replace("key",key.toString());
    response.replace("keyHref",encode(key));
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
