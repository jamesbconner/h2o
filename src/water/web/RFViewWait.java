
package water.web;

import java.util.Properties;

/**
 *
 * @author peta
 */
public class RFViewWait extends H2OPage {

  public static final String html="<form id='rfw' name='rfw' action='RFView' method='get'>%arg{<input type='hidden' name='%name' value='%value'/>}</form>"
          + "<div class='alert alert-info'><b>Please wait!</b> It may take some time to calculate the confusion matrix.</div>"
          + "<script>\n"
          + "document.forms['rfw'].submit()\n"
          + "</script>";


  @Override
  protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = new RString(html);
    for (String s: args.stringPropertyNames()) {
      RString x = result.restartGroup("arg");
      x.replace("name",s);
      x.replace("value",args.getProperty(s));
      x.append();
    }
    return result.toString();
  }

}
