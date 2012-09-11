package water.web;

import hexlytics.rf.Confusion;
import hexlytics.rf.DRF;
import hexlytics.rf.Tree.StatType;

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
    int gini = getAsNumber(p, "gini", StatType.ENTROPY.ordinal());
    StatType statType = StatType.values()[gini];

    // Start the distributed Random Forest
    DRF drf = hexlytics.rf.DRF.web_main(ary,ntrees,depth,-1.0,statType,false/*non-blocking*/);

    // Start up the incremental confusion matrix
    Confusion confusion = new Confusion( drf._treeskey, ary, ntrees*H2O.CLOUD.size());
    Key confKey = confusion.toKey();

    JsonObject res = new JsonObject();
    res.addProperty("h2o",H2O.SELF.urlEncode());
    res.addProperty("confKey",encode(confKey));
    res.addProperty("depth",depth);
    return res;
  }

  @Override protected String serveImpl(Server s, Properties p) throws PageError {
    JsonObject json = serverJson(s, p);

    RString response = new RString(html);
    response.replace(json);
    return response.toString();
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=http:/%h2o/RFView?Key=%confKey&depth=%depth\">\n";
}
