package water.web;
import java.util.Properties;
import water.*;
import hexlytics.tests.PokerDRF;

public class RandomForest extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    String skey1 = args.getProperty("Key");
    Key key1 = Key.make(skey1);    
    //String res = PokerDRF.doit(key1);
    String res = PokerDRF.webrun(key1);
    RString response = new RString(html);
    response.replace("key",key1);
    response.replace("res",res);
    return response.toString();
  }
  final static String html =
    "\n<p>Random Forest of %key results:\n<p>%res\n";
}
