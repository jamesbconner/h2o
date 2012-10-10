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
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    ValueArray ary = ServletUtil.check_array(p,"Key");
    int ntrees = getAsNumber(p,"ntrees", 5);
    int depth = getAsNumber(p,"depth", 30);
    int gini = getAsNumber(p, "gini", StatType.GINI.ordinal());
    int singlethreaded =  getAsNumber(p,"singlethreaded", 1);
    int seed =  getAsNumber(p,"seed", 42);
    StatType statType = StatType.values()[gini];

    // Optionally, save the model
    Key modelKey = null;
    String skey = p.getProperty("modelKey","____model");
    try {
      modelKey = H2OPage.decode(skey);
      System.out.println("modelKey="+modelKey);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }

    // Start the distributed Random Forest

    JsonObject res = new JsonObject();
    res.addProperty("h2o",H2O.SELF.urlEncode());
    try {
      DRF drf = hex.rf.DRF.web_main(ary,ntrees,depth,-1.0,statType,seed,singlethreaded==0/*non-blocking*/, modelKey);
      // Start up the incremental confusion matrix
      Confusion confusion = new Confusion( drf._treeskey, ary._key,  seed);
      Key confKey = confusion.toKey();
      addProperty(res, "confKey", confKey);
      addProperty(res, "modelKey", modelKey);
      res.addProperty("depth",depth);
    } catch(DRF.IllegalDataException e) {
      res.addProperty("error", H2OPage.error("Incorrect input data: " + e.getMessage()));
    }
    return res;

  }

  @Override protected String serveImpl(Server s, Properties p, String sessionID) throws PageError {
    JsonObject json = serverJson(s, p, sessionID);
    if(!json.has("error")){
      RString response = new RString(html);
      response.replace(json);
      return response.toString();
    }
    return H2OPage.error(json.get("error").toString());
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=/RFView?Key=%confKeyHref&modelKey=%modelKeyHref&depth=%depth\">\n";
}
