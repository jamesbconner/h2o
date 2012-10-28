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
    int ntree = getAsNumber(p,"ntree", 5);
    int depth = getAsNumber(p,"depth", 30);
    int gini = getAsNumber(p, "gini", StatType.GINI.ordinal());
    int singlethreaded =  getAsNumber(p,"singlethreaded", 1);
    int seed = getAsNumber(p,"seed", 42);
    StatType statType = StatType.values()[gini];

    // Optionally, save the model
    Key modelKey = null;
    String skey = p.getProperty("modelKey","model");
    try {
      modelKey = Key.make(skey);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }

    // Pick the column to classify
    int classcol = ary.num_cols()-1; // Default to the last column
    String clz = p.getProperty("class");
    if( clz != null ) {
      int[] clarr = parseVariableExpression(ary.col_names(), clz);
      if( clarr.length != 1 )
        throw new InvalidInputException("Class has to refer to exactly one column!");
      classcol = clarr[0];
      if( classcol < 0 || classcol >= ary.num_cols() )
        throw new InvalidInputException("Class out of range");
    }

    // Pick columns to ignore
    String igz = p.getProperty("ignore");
    int[] ignores =  igz == null ? new int[0] : parseVariableExpression(ary.col_names(), igz);

    // Start the distributed Random Forest

    JsonObject res = new JsonObject();
    res.addProperty("h2o",H2O.SELF.urlEncode());
    try {
      DRF drf = hex.rf.DRF.web_main(ary,ntree,depth,-1.0,statType,seed,singlethreaded==0/*non-blocking*/, classcol,ignores,modelKey);
      // Output a model with zero trees (so far).
      final int classes = (short)((ary.col_max(classcol) - ary.col_min(classcol))+1);
      Model model = new Model(modelKey,drf._treeskey,ary.num_cols(),classes);
      // Save it to the cloud
      UKV.put(modelKey,model);

      // Pass along all to the viewer
      addProperty(res,"dataKey" , ary._key);
      addProperty(res,"treesKey", drf._treeskey);
      addProperty(res,"modelKey", modelKey);
      res.addProperty("ntree", ntree);
      res.addProperty("class", classcol);
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
    "<meta http-equiv=\"REFRESH\" content=\"0;url=/RFView?dataKey=%$dataKey&modelKey=%$modelKey&treesKey=%$treesKey&ntree=%ntree&class=%class\">\n";
}
