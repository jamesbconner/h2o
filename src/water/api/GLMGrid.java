package water.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import hex.*;
import hex.GLMSolver.*;
import java.util.UUID;
import water.*;
import water.web.RString;

/**
 * @author cliffc
 */
public class GLMGrid extends Request {
  public static final String JSON_GLM_Y = "y";
  public static final String JSON_GLM_X = "x";
  public static final String JSON_GLM_NEG_X = "neg_x";
  public static final String JSON_GLM_FAMILY = "family";
  public static final String JSON_GLM_NORM = "norm";
  public static final String JSON_GLM_LAMBDA = "lambda_1";
  public static final String JSON_GLM_LAMBDA_2 = "lambda_2";
  public static final String JSON_GLM_RHO = "rho";
  public static final String JSON_GLM_ALPHA = "alpha";
  public static final String JSON_GLM_MAX_ITER = "max_iter";
  public static final String JSON_GLM_BETA_EPS = "beta_eps";
  public static final String JSON_GLM_WEIGHT = "weight";
  public static final String JSON_GLM_THRESHOLD = "threshold";
  public static final String JSON_GLM_XVAL = "xval";
  public static final String JSON_GLM_CASE = "case";
  public static final String JSON_GLM_LINK = "link";

  public static final String JSON_ROWS = "rows";
  public static final String JSON_TIME = "time";
  public static final String JSON_COEFFICIENTS = "coefficients";

  // Need a HEX key for GLM
  protected final H2OHexKey _key = new H2OHexKey(KEY);
  // Column to classify on
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(_key, JSON_GLM_Y);
  // Columns used to run the GLM
  protected final IgnoreHexCols _x = new IgnoreHexCols2(_key, _y, JSON_GLM_X);

  // Args NOT Grid Searched
  protected final Int _maxIter = new Int(JSON_GLM_MAX_ITER, GLMSolver.DEFAULT_MAX_ITER, 1, 1000000);
  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);
  protected final Real _case = new Real(JSON_GLM_CASE, Double.NaN);
  protected final EnumArgument<Link> _link = new EnumArgument(JSON_GLM_LINK,Link.familyDefault);
  protected final Int _xval = new Int(JSON_GLM_XVAL, 10, 0, 1000000);
  protected final Real _betaEps = new Real(JSON_GLM_BETA_EPS,GLMSolver.DEFAULT_BETA_EPS);

  // Args that ARE Grid Searched
  protected final Str _lambda1 = new Str(JSON_GLM_LAMBDA, ""+LSMSolver.DEFAULT_LAMBDA);
  protected final Str _lambda2 = new Str(JSON_GLM_LAMBDA_2, ""+LSMSolver.DEFAULT_LAMBDA2);
  protected final Str _alpha = new Str(JSON_GLM_ALPHA, ""+LSMSolver.DEFAULT_ALPHA);
  protected final Str _rho = new Str(JSON_GLM_RHO, ""+LSMSolver.DEFAULT_RHO);
  protected final Str _threshold = new Str(JSON_GLM_THRESHOLD, ""+0.5);


  // ---
  // Make a new Grid Search object.
  @Override protected Response serve() {
    // The "task key" for this Grid search.  Used to track job progress, to
    // shutdown early, to collect best-so-far & grid results, etc.  Pinned to
    // self, because it's almost always updated locally.
    Key taskey = Key.make("Task"+UUID.randomUUID().toString(),(byte)0,Key.TASK,H2O.SELF);
    GLMGridStatus task = 
      new GLMGridStatus(taskey,       // Self/status/task key
                        _key.value(), // Hex data
                        _y.value(),   // Column to classify
                        _x.value(),   // Columns to run GLM over
                        parsePRange( true, _lambda1.value()),   // Grid ranges
                        parsePRange( true, _lambda2.value()),   // Grid ranges
                        parsePRange( true, _rho.value()),       // Grid ranges
                        parsePRange( true, _alpha.value()),     // Grid ranges
                        parsePRange(false, _threshold.value()));// Grid ranges

    // Put the task Out There for all to find
    UKV.put(taskey,task);
    // Start the grid search
    assert task._working == true;
    H2O.FJP_NORM.submit(task);

    // Redirect to the grid-search status page
    JsonObject j = new JsonObject();
    j.addProperty(Constants.DEST_KEY, taskey.toString());
    Response r = GLMGridProgress.redirect(j, taskey);
    r.setBuilder(Constants.DEST_KEY, new KeyElementBuilder());
    return r;
  }

  // ----
  // TODO: Move this into a argument 'checker'
  protected static double [] parsePRange( boolean multiply, String str ) {
    str = str.trim().toLowerCase();
    if( str.startsWith("seq") ) {
      throw new Error("unimplemented");
    } if( str.contains(":") ) {
      String [] parts = str.split(":");
      if( parts.length != 3 )throw new Error("unexpected sequence format \"" + str + "\"");
      double from = Double.parseDouble(parts[0]);
      double to = Double.parseDouble(parts[1]);
      double step = Double.parseDouble(parts[2]);
      if(to == from) return new double[]{from};
      if(to < from)throw new Error("");
      if(step == 0)throw new Error();
      int n = multiply 
        ? (int)((Math.log(to) - Math.log(from))/Math.log(step))
        : (int)((         to  -          from )/         step );
      double [] res = new double[n];
      for( int i = 0; i < n; ++i ) {
        res[i] = from;
        if( multiply ) from *= step; else from += step;
      }
      return res;
    } else if( str.contains(",") ) {
      String [] parts = str.split(",");
      double [] res = new double[parts.length];
      for(int i = 0; i < parts.length; ++i)
        res[i] = Double.parseDouble(parts[i]);
      return res;
    } else {
      return new double [] {Double.parseDouble(str)};
    }
  }

  // Make a link that lands on this page
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GLMGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
}
