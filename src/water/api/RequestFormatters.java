
package water.api;

import com.google.gson.JsonElement;

/**
 *
 * @author peta
 */
public class RequestFormatters extends RequestArguments {

  public abstract class Formatter {

    Formatter _chain = null;

    public abstract String format(String element, String elementName, String elementContext);


    protected String build(JsonElement element, String elementName, String elementContext) {
      // first do our stuff and then chain it back
      String result = format(element.toString(),elementName, elementContext);
      Formatter chain = _chain;
      while (chain != null) {
        result = chain.format(result, elementName, elementContext);
        chain = chain._chain;
      }
      return result;
    }
  }

  public abstract static class ObjectFormatter extends Formatter {



    protected String build(JsonElement element, String elementName, String elementContext) {
      
    }

  }






}
