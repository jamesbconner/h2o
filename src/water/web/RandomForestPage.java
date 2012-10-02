package water.web;

import hex.rf.Confusion;
import hex.rf.DRF;
import hex.rf.Tree.StatType;

import java.util.Properties;

import water.H2O;
import water.Key;
import water.ValueArray;

import com.google.gson.JsonObject;

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
    int gini = getAsNumber(p, "gini", StatType.GINI.ordinal());
    int singlethreaded =  getAsNumber(p,"singlethreaded", 1);
    int seed =  getAsNumber(p,"seed", 42);
    StatType statType = StatType.values()[gini];

    // Start the distributed Random Forest

    JsonObject res = new JsonObject();
    res.addProperty("h2o",H2O.SELF.urlEncode());
    try {
      DRF drf = hex.rf.DRF.web_main(ary,ntrees,depth,-1.0,statType,seed,singlethreaded==0/*non-blocking*/);
      // Start up the incremental confusion matrix
      Confusion confusion = new Confusion( drf._treeskey, ary, ntrees*H2O.CLOUD.size(), seed);
      Key confKey = confusion.toKey();
      addProperty(res, "confKey", confKey);
      res.addProperty("depth",depth);
    } catch(DRF.IllegalDataException e) {
      res.addProperty("error", H2OPage.error("Incorrect input data: " + e.getMessage()));
    }
    return res;

  }

  @Override protected String serveImpl(Server s, Properties p) throws PageError {
    JsonObject json = serverJson(s, p);
    if(!json.has("error")){
      RString response = new RString(html);
      response.replace(json);
      return response.toString();
    }
    return H2OPage.error(json.get("error").toString());
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=/RFView?Key=%confKeyHref&depth=%depth\">\n";
}
