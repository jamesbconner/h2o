package water.web;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Properties;

/** H2O branded web page.
 * 
 * We might in theory not need this and it can be all pushed to Page, but this
 * is just for the sake of generality. 
 *
 * @author peta
 */
public abstract class H2OPage extends Page {

  protected int _refresh = 0;
  
  protected abstract String serve_impl(Properties args);
  
  private static final String html =
      "<!DOCTYPE html>"
    + "<html lang=\"en\">"
    + "  <head>"
    + "    <meta charset=\"utf-8\">"
    + "    %refresh"
    + "    <title>H2O, from 0xdata</title>"
    + "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"
    + "    <link href=\"bootstrap/css/bootstrap.css\" rel=\"stylesheet\">"
    + "    <style>"
    + "      body {"
    + "        padding-top: 60px; /* 60px to make the container go all the way to the bottom of the topbar */"
    + "      }"
    + "    </style>"
    + "    <link href=\"bootstrap/css/bootstrap-responsive.css\" rel=\"stylesheet\">"
    + "    <!-- Le HTML5 shim, for IE6-8 support of HTML5 elements -->"
    + "    <!--[if lt IE 9]>"
    + "      <script src=\"http://html5shim.googlecode.com/svn/trunk/html5.js\"></script>"
    + "    <![endif]-->"
    + "    <!-- Le fav and touch icons -->"
    + "    <link rel=\"shortcut icon\" href=\"favicon.ico\">"
    + "    <script src='bootstrap/js/jquery.js'></script>"
    + "  </head>"
    + "  <body>"
    + "    <div class=\"navbar navbar-fixed-top\">"
    + "      <div class=\"navbar-inner\">"
    + "        <div class=\"container\">"
    + "          <a class=\"btn btn-navbar\" data-toggle=\"collapse\" data-target=\".nav-collapse\">"
    + "            <span class=\"icon-bar\"></span>"
    + "            <span class=\"icon-bar\"></span>"
    + "            <span class=\"icon-bar\"></span>"
    + "          </a>"
    + "          <a class=\"brand\" href=\"#\">H<sub>2</sub>O</a>"
    + "          <div class=\"nav\">"
    + "            <ul class=\"nav\">"
    + "              <li><a href=\"/\">Cloud</a></li>"
    + "              <li><a href=\"/StoreView\">Node</a></li>"
    + "              <li><a href=\"/GetQuery\">Get</a></li>"
    + "              <li><a href=\"/Put\">Put</a></li>"
    + "              <li><a href=\"/AppendQuery\">Append</a></li>"
    + "              <li><a href=\"/Timeline\">Timeline</a></li>"
    + "              <li><a href=\"/ImportQuery\">Import</a></li>"
    + "              <li><a href=\"/DebugView\">Debug View</a></li>"
    + "              <li><a href=\"/Network\">Network</a></li>"
    + "              <li><a href=\"/Shutdown\">Shutdown All</a></li>"
    + "            </ul>"
    + "          </div><!--/.nav-collapse -->"
    + "        </div>"
    + "      </div>"
    + "    </div>"
    + "    <div class=\"container\">"
    + "%contents"
    + "    </div>"
    + "  </body>"
    + "</html>"
    ;
        
//  private static RString response = new RString(html); 
  
  @Override public String serve(Server server, Properties args) {
    RString response = new RString(html);
//    response.clear();
    String result = serve_impl(args);
    if (result == null)
      return result;
    if (_refresh!=0)
      response.replace("refresh","<META HTTP-EQUIV='refresh' CONTENT='"+_refresh+"'>");
    response.replace("contents",result);
    return response.toString();
  }
  
  
  private static final String html_notice = 
            "<div class='alert %atype'>"
          + "%notice"
          + "</div>"
          ;
  
//  private static final RString notice = new RString(html_notice);
  
  public static String error(String text) {
    RString notice = new RString(html_notice);
//    notice.clear();
    notice.replace("atype","alert-error");
    notice.replace("notice",text);
    return notice.toString();
  }

  public static String success(String text) {
    RString notice = new RString(html_notice);
//    notice.clear();
    notice.replace("atype","alert-success");
    notice.replace("notice",text);
    return notice.toString();
  }
  
  public static String urlEncode(String what) {
    try {
      return URLEncoder.encode(what,"UTF-8");
    } catch (UnsupportedEncodingException e) {
      // pass
      return null;
    }
  }
  
  public static String urlDecode(String what) {
    try {
      return URLDecoder.decode(what,"UTF-8");
    } catch (UnsupportedEncodingException e) {
      // pass
      return null;
    }
  }
  
  public static String wrap(String what) {
    RString response = new RString(html);
//    response.clear();
    response.replace("contents",what);
    return response.toString();
  }
  
  public static int getAsNumber(Properties args, String arg, int def) {
    int result = def;
    try {
      String s = args.getProperty(arg,"");
      if (!s.isEmpty())
        result = Integer.parseInt(s);
    } catch (NumberFormatException e) {
      return def;
    }
    return result;
  }
  
}
