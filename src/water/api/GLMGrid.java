package water.api;

import com.google.gson.*;
import java.util.BitSet;
import hex.GLMSolver.*;
import hex.GLMSolver;
import hex.LSMSolver.Norm;
import hex.LSMSolver;
import water.*;

/**
 * @author cliffc
 */
public class GLMGrid extends Request {

  public static final String JSON_GLM_Y = "y";
  public static final String JSON_GLM_X = "x";
  public static final String JSON_GLM_THRESHOLD = "threshold";

  // The model# counter, used when stepping through many models.  Needs to
  // become a thread-local to support multiple unrelated grid-searches.
  private static int _counter;

  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(_key, JSON_GLM_Y);
  protected final IgnoreHexCols _x = new IgnoreHexCols2(_key, _y, JSON_GLM_X);

  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _key.value();
      int Y = _y.value();
      int[] columns = createColumns();
      int lim=columns.length;

      res.addProperty("key", ary._key.toString());
      res.addProperty("h2o", H2O.SELF.toString());
      res.addProperty("counter", _counter);
      res.addProperty("cols", new Gson().toJson(columns));
      
      Response r = _counter == lim 
        ? Response.done(res) 
        : Response.poll(res,_counter++,lim);
      r.setBuilder(""/*top-level do-it-all builder*/,new GridBuilder());
      return r;

    } catch (Throwable t) {
      t.printStackTrace();
      return Response.error(t.getMessage());
    }
  }

  private  int[] createColumns() {
    BitSet cols = new BitSet();
    for( int i : _x.value() ) cols.set(i);
    int[] res = new int[cols.cardinality()+1];
    int x=0;
    for( int i = cols.nextSetBit(0); i >= 0; i = cols.nextSetBit(i+1))
      res[x++] = i;
    res[x] = _y.value();
    return res;
  }


  private static class GridBuilder extends ObjectBuilder {
    public String build(Response response, JsonObject json, String contextName) {
      String ss = new Gson().fromJson(json.get("cols"),String.class);
      int[] cols = new Gson().fromJson(ss,int[].class);
      return "grid builder goes here: col "+(cols==null?"null":(""+cols[_counter]));
    }
  }

}
