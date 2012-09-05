package water.web;
import java.util.Properties;

import water.H2O;
import water.ValueArray;

public class RandomForest extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    Object o = ServletUtil.check_array(args,"Key");
    if( o instanceof String ) return (String)o;
    ValueArray ary = (ValueArray)o;
    int ntrees = getAsNumber(args,"ntrees", 5*H2O.CLOUD._memary.length);
    int depth = getAsNumber(args,"depth", 30);
    boolean gini = args.getProperty("gini")!=null;
    
    String res = "some results go here";
    try { hexlytics.rf.RandomForest.web_main(ary,ntrees,depth,gini); }
    catch( Exception e ) { res = e.toString(); }
    RString response = new RString(html);
    response.replace("key",ary._key);
    response.replace("res",res);
    return response.toString();
  }
  final static String html =
    "\n<p>Random Forest of %key results:\n<p>%res\n";
}
