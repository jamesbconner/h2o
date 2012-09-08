package water.web;

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
    // default gini is on.
    int gini = getAsNumber(args, "gini", 1);
    String statType = gini == 1 ? Tree.GINI : Tree.ENTROPY;
    Key treeskey;
    try {
      treeskey = hexlytics.rf.DRF.web_main(ary,ntrees,depth,-1.0,statType);
    } catch( Exception e ) {
      return wrap(error(e.toString()));
    }
    RString response = new RString(html);
    response.replace("h2o",H2O.SELF.urlEncode());
    response.replace("treeskey",treeskey);
    response.replace("ntrees",ntrees*H2O.CLOUD.size());
    response.replace("depth",depth);
    response.replace("origKey",ary._key);
    return response.toString();
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=http:/%h2o/RFView?Key=%treeskey&ntrees=%ntrees&depth=%depth&origKey=%origKey\">\n";
}
