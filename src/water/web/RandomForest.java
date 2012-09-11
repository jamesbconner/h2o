package water.web;

import hexlytics.rf.Confusion;
import hexlytics.rf.DRF;
import hexlytics.rf.Tree;
import java.util.Properties;
import water.H2O;
import water.Key;
import water.ValueArray;

// @author cliffc
public class RandomForest extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    Object o = ServletUtil.check_array(args,"Key");
    if( o instanceof String ) return (String)o;
    ValueArray ary = (ValueArray)o;
    int ntrees = getAsNumber(args,"ntrees", 5);
    int depth = getAsNumber(args,"depth", 30);
    String gini = args.getProperty("gini");
    Tree.StatType stat = "gini".equals(gini) ? Tree.StatType.gini : Tree.StatType.numeric;

    // Start the distributed Random Forest
    DRF drf = hexlytics.rf.DRF.web_main(ary,ntrees,depth,-1.0,stat);
    // Start up the incremental confusion matrix
    Confusion confusion = new Confusion( drf._treeskey, ary, ntrees*H2O.CLOUD.size());
    Key confkey = confusion.toKey();

    RString response = new RString(html);
    response.replace("h2o",H2O.SELF.urlEncode());
    response.replace("confkey",encode(confkey._kb));
    response.replace("depth",depth);
    return response.toString();
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=http:/%h2o/RFView?Key=%confkey&depth=%depth\">\n";
}
