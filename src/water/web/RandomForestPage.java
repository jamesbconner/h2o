package water.web;

import com.google.gson.JsonObject;
import hex.rf.*;
import hex.rf.Tree.StatType;
import java.util.Properties;
import water.*;

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
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }

    // Start the distributed Random Forest

    JsonObject res = new JsonObject();
    res.addProperty("h2o",H2O.SELF.urlEncode());
    try {
      DRF drf = hex.rf.DRF.web_main(ary,ntrees,depth,-1.0,statType,seed,singlethreaded==0/*non-blocking*/);
      // Output a model with zero trees (so far).
      final int num_cols = ary.num_cols();
      final short classes = (short)((ary.col_max(num_cols-1) - ary.col_min(num_cols-1))+1);
      Model model = new Model(modelKey,drf._treeskey,num_cols,classes);
      // Save it to the cloud
      UKV.put(modelKey,model);

      // Pass along all to the viewer
      addProperty(res, "dataKey", ary._key);
      addProperty(res, "treesKey", drf._treeskey);
      addProperty(res, "modelKey", modelKey);
      res.addProperty("ntrees", ntrees);
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
    "<meta http-equiv=\"REFRESH\" content=\"0;url=/RFView?dataKey=%dataKeyHref&modelKey=%modelKeyHref&treesKey=%treesKeyHref&ntrees=%ntrees\">\n";
}
