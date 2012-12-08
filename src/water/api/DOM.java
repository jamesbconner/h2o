
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
  public static String success(String message) {
    return "<div class='alert alert-success'>"
            + message
            +"</div>";
  }

  /** Appends the nicely formatted info message. Blue. */
  public static String info(String message) {
    return "<div class='alert alert-info'>"
            + message
            +"</div>";
  }

  /** Appends the nicely formatted warning message. Orange.*/
  public static String warning(String message) {
    return "<div class='alert'>"
            + message
            +"</div>";
  }

  /** Appends the nicely formatted error message. Red. */
  public static String error(String message) {
    return "<div class='alert alert-error'>"
            + message
            +"</div>";
  }

  /** Returns the horizontal description list. This should be used to display
   * simple JSON values. */
  public static String dlHorizontal(String header, String value) {
    return "<dl class='dl-horizontal'>"
            + "<dt>"
            + header
            + "</dt><dd>"
            + value
            + "</dd></dl>";
  }

  public static String td(String innerHTML) {
    return "<td>" + innerHTML + "</td>";
  }

  public static String urlEncode(String what) {
    try {
      return URLEncoder.encode(what,"UTF-8");
    } catch( UnsupportedEncodingException ex ) {
      return what;
    }

  }

  public static String p(String innerHTML) {
    return "<p>"+innerHTML+"</p>";
  }

  public static String color(String what, String color) {
    return "<span style='color:"+color+"'>"+what+"</span>";
  }

  public static String a(String name, String href) {
    return "<a href=\'"+href+"'>"+name+"</a>";
  }

  public static String h3(String name) {
    return "<h3>"+name+"</h3>";
  }

  public static String textInput(String name, String value, String placeholder) {
    return "<input class='span5' type='text' name='"+name+"' id='"+name+"' placeholder='"+placeholder+"' "+ (((value!=null) && (!value.isEmpty())) ? (" value='"+value+"' />") : "/>");
  }
}
