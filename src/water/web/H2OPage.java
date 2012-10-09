package water.web;

import java.util.Properties;

import com.google.gson.JsonObject;

import water.Key;

/** H2O branded web page.
 *
 * We might in theory not need this and it can be all pushed to Page, but this
 * is just for the sake of generality.
 *
 * @author peta
 */
public abstract class H2OPage extends Page {

  protected int _refresh = 0;

  protected abstract String serveImpl(Server server, Properties args, String sessionID) throws PageError;

  protected String[] additionalScripts() { return EMPTY; }
  protected String[] additionalStyles()  { return EMPTY; }

  private static final String navbar =
      "              <li><a href=\"/\">Cloud</a></li>"
    + "              <li><a href=\"/StoreView\">Node</a></li>"
    + "              <li><a href=\"/GetQuery\">Get</a></li>"
    + "              <li><a href=\"/Put\">Put</a></li>"
    + "              <li><a href=\"/Timeline\">Timeline</a></li>"
    + "              <li><a href=\"/ImportQuery\">Import</a></li>"
    + "              <li><a href=\"/DebugView\">Debug View</a></li>"
    + "              <li><a href=\"/ProgressView\">Progress View</a></li>"
    + "              <li><a href=\"/Network\">Network</a></li>"
    + "              <li><a href=\"/Shutdown\">Shutdown All</a></li>"
    ;

  @Override public String serve(Server server, Properties args, String sessionID) {
    String username = sessionID != null ? server._sessionManager.authenticate(sessionID) : null;
    RString response = new RString(html);
    response.replace("navbar",navbar);
    if (username != null)
      response.replace("footer","You are logged as "+username+". <a href=\"logoff\">Logoff</a>");
    try {
      String result = serveImpl(server, args, sessionID);
      if (result == null) return result;
      if (_refresh!=0) response.replace("refresh","<META HTTP-EQUIV='refresh' CONTENT='"+_refresh+"'>");

      // Append additional scripts
      StringBuilder additions = new StringBuilder();
      for(String script : additionalScripts()) {
        additions.append("<script src='");additions.append(script);additions.append("'></script>");
      }
      response.replace("scripts", additions.toString());
      // Append additional styles <link href=\"bootstrap/css/bootstrap.css\" rel=\"stylesheet\">"
      additions.setLength(0);
      for(String style : additionalStyles()) {
        additions.append("<link href='");additions.append(style);additions.append("' rel='stylesheet'>");
      }
      response.replace("styles", additions.toString());

      response.replace("contents",result);
    } catch (PageError e) {
      response.replace("contents", e._msg);
    }
    return response.toString();
  }


  private static final String html_notice =
            "<div class='alert %atype'>"
          + "%notice"
          + "</div>"
          ;

  public static String error(String text) {
    RString notice = new RString(html_notice);
    notice.replace("atype","alert-error");
    notice.replace("notice",text);
    return notice.toString();
  }

  public static String success(String text) {
    RString notice = new RString(html_notice);
    notice.replace("atype","alert-success");
    notice.replace("notice",text);
    return notice.toString();
  }

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  public static String encode(Key k) {
    byte[] what = k._kb;
    int len = what.length;
    while( --len >= 0 ) {
      char a = (char) what[len];
      if( a == '-' ) continue;
      if( a == '.' ) continue;
      if( 'a' <= a && a <= 'z' ) continue;
      if( 'A' <= a && a <= 'Z' ) continue;
      if( '0' <= a && a <= '9' ) continue;
      break;
    }
    StringBuilder sb = new StringBuilder();
    for( int i = 0; i <= len; ++i ) {
      byte a = what[i];
      sb.append(HEX[(a >> 4) & 0x0F]);
      sb.append(HEX[(a >> 0) & 0x0F]);
    }
    sb.append("____");
    for( int i = len + 1; i < what.length; ++i ) sb.append((char)what[i]);
    return sb.toString();
  }

  public static Key decode(String what) {
    int len = what.indexOf("____");
    String tail = what.substring(len + 4);
    int r = 0;
    byte[] res = new byte[len/2 + tail.length()];
    for( int i = 0; i < len; i+=2 ) {
      char h = what.charAt(i);
      char l = what.charAt(i+1);
      h -= Character.isDigit(h) ? '0' : ('a' - 10);
      l -= Character.isDigit(l) ? '0' : ('a' - 10);
      res[r++] = (byte)(h << 4 | l);
    }
    System.arraycopy(tail.getBytes(), 0, res, r, tail.length());
    return Key.make(res);
  }

  public static void addProperty(JsonObject json, String k, Key key) {
    json.addProperty(k, key.toString());
    json.addProperty(k + "Href", encode(key));
  }

  public static String wrap(String what) {
    RString response = new RString(html);
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

  protected static final String[] EMPTY = {};
}
