
package water.api.html;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import init.Boot;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import water.Log;

/** Basic class for a HTML page.
 *
 * This class both defines a common behavior for all HTML pages and serves as
 * a static container for the utility functions that can be used to faster
 * writing and editing of the web pages.
 *
 * @author peta
 */
public class HtmlPage {

  private static class PageInfo {
    public final String _name;
    public final String _href;
    public PageInfo(String name, String href) {
      _name = name;
      _href = href;
    }
  }

  private static String _html;

  private static HashMap<String, ArrayList<PageInfo> > _navbarPages = new HashMap();


  {
    InputStream resource = Boot._init.getResource2("/page.html");
    try {
      _html = new String(ByteStreams.toByteArray(resource));
    } catch (NullPointerException e) {
      Log.die("page.html not found in resources.");
    } catch (Exception e) {
      Log.die(e.getMessage());
    } finally {
      Closeables.closeQuietly(resource);
    }
  }

  /** Adds the given page */
  private static HtmlPage addToNavbar(HtmlPage page, String section) {
    if (_navbarPages.containsKey(section))
      _navbarPages.put(section, new ArrayList<PageInfo>());
//    _navbarPages.get(section).add(new PageInfo(page));
    return page;
  }

  private static HtmlPage addToNavbar(HtmlPage page) {
    return addToNavbar(page,"");
  }


  

  // non statics for the html pages api creation


  public String serve() {
    return _html;
  }

}
