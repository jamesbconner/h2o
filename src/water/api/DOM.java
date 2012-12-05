
package water.api;

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
}
