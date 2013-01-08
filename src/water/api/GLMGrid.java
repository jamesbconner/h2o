package water.api;

import hex.*;
import hex.GLMSolver.Link;

import java.util.UUID;

import water.*;
import water.web.RString;

import com.google.gson.JsonObject;

/**
 * @author cliffc
 */
public class GLMGrid extends Request {
  public static final String JSON_GLM_Y = "y";
  public static final String JSON_GLM_X = "x";
  public static final String JSON_GLM_FAMILY = "family";
  public static final String JSON_GLM_NORM = "norm";
  public static final String JSON_GLM_MAX_ITER = "max_iter";
  public static final String JSON_GLM_BETA_EPS = "beta_eps";
  public static final String JSON_GLM_WEIGHT = "weight";

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
  protected final CaseSelect _case = new CaseSelect(_key,_y,JSON_GLM_CASE);
  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);
  protected final EnumArgument<Link> _link = new EnumArgument(JSON_GLM_LINK,Link.familyDefault);
  protected final Int _xval = new Int(JSON_GLM_XVAL, 10, 0, 1000000);
  protected final Real _betaEps = new Real(JSON_GLM_BETA_EPS,GLMSolver.DEFAULT_BETA_EPS);

  // Args that ARE Grid Searched
  protected final RSeq _lambda1 = new RSeq(Constants.LAMBDA_1, false, new NumberSequence("1e-8:1e3:100",true,10),true);
  protected final RSeq _lambda2 = new RSeq(Constants.LAMBDA_2, false,new NumberSequence("1e-8:1e3:100",true,10),true);
  protected final RSeq _alpha = new RSeq(Constants.ALPHA, false, new NumberSequence("1,1.4,1.8",false,1),false);
  protected final RSeq _rho = new RSeq(Constants.RHO, false,new NumberSequence("1e-8:1e3:100",true,10),true);
  protected final RSeq _thresholds = new RSeq(Constants.DTHRESHOLDS, false,new NumberSequence("0:1:0.01",false,0.1),false);

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
                        _lambda1.value().arr,   // Grid ranges
                        _lambda2.value().arr,   // Grid ranges
                        _rho.value().arr,       // Grid ranges
                        _alpha.value().arr,     // Grid ranges
                        _thresholds.value().arr,
                        _case.value(), _xval.value());

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


  // Make a link that lands on this page
  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GLMGrid.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }
}
