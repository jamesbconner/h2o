package water.web;

import hexlytics.rf.Tree.StatType;

import java.util.Properties;

import com.google.gson.JsonObject;

import water.H2O;
import water.Key;
import water.ValueArray;
import water.web.Page.PageError;

// @author cliffc
public class RandomForestPage extends H2OPage {
  @Override
  public String[] requiredArguments() {
    return new String[] { "Key" };
  }

  @Override
  public JsonObject serverJson(Server s, Properties p) throws PageError {
    ValueArray ary = ServletUtil.check_array(p,"Key");
    int ntrees = getAsNumber(p,"ntrees", 5);
    int depth = getAsNumber(p,"depth", 30);
    // default gini is on.
    int gini = getAsNumber(p, "gini", StatType.GINI.ordinal());
    StatType statType = StatType.values()[gini];
    Key treeskey;
    try {
      treeskey = hexlytics.rf.DRF.web_main(ary,ntrees,depth,-1.0,statType);
    } catch( Exception e ) {
      throw new PageError(e.toString());
    }

    JsonObject res = new JsonObject();
    res.addProperty("h2o",H2O.SELF.urlEncode());
    res.addProperty("treeskey",encode(treeskey));
    res.addProperty("ntrees",ntrees*H2O.CLOUD.size());
    res.addProperty("depth",depth);
    res.addProperty("origKey",encode(ary._key));
    return res;
  }

  @Override protected String serveImpl(Server s, Properties p) throws PageError {
    JsonObject json = serverJson(s, p);

    RString response = new RString(html);
    response.replace(json);
    return response.toString();
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=http:/%h2o/RFView?Key=%treeskey&ntrees=%ntrees&depth=%depth&origKey=%origKey\">\n";
}
