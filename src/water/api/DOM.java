
package water.api;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import init.Boot;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.logging.Level;
import java.util.logging.Logger;
import water.Log;

/** Mostly static class used to
 *
 * @author peta
 */
public class DOM {

  /** Appends the nicely formatted success message. Green. */
  public static void success(StringBuilder sb, String message) {
    sb.append("<div class='alert alert-success'>");
    sb.append(message);
    sb.append("</div>");
  }

  /** Appends the nicely formatted info message. Blue. */
  public static void info(StringBuilder sb, String message) {
    sb.append("<div class='alert alert-info'>");
    sb.append(message);
    sb.append("</div>");
  }

  /** Appends the nicely formatted warning message. Orange.*/
  public static void warning(StringBuilder sb, String message) {
    sb.append("<div class='alert'>");
    sb.append(message);
    sb.append("</div>");
  }

  /** Appends the nicely formatted error message. Red. */
  public static void error(StringBuilder sb, String message) {
    sb.append("<div class='alert alert-error'>");
    sb.append(message);
    sb.append("</div>");
  }

  /** Returns the horizontal description list. This should be used to display
   * simple JSON values. */
  public static void dlHorizontal(StringBuilder sb, String header, String value) {
    sb.append("<dl class='dl-horizontal'>");
    sb.append("<dt>");
    sb.append(header);
    sb.append("</dt><dd>");
    sb.append(value);
    sb.append("</dd></dl>");
  }

  public static void td(StringBuilder sb, String innerHTML) {
    sb.append("<td>");
    sb.append(innerHTML);
    sb.append("</td>");
  }

  public static String urlEncode(String what) {
    try {
      return URLEncoder.encode(what,"UTF-8");
    } catch( UnsupportedEncodingException ex ) {
      return what;
    }

  }

  public static String a(String name, String href) {
    return "<a href=\'"+href+"'>"+name+"</a>";
  }
}
