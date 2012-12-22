package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import hex.GLMSolver;
import hex.GLMSolver.*;
import hex.LSMSolver;
import hex.LSMSolver.Norm;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import water.H2O;
import water.ValueArray;
import water.web.RString;
import water.web.ServletUtil;

/**
 *
 * @author peta
 */
public class GLM extends Request {

  public static final String JSON_GLM_Y = "y";
  public static final String JSON_GLM_X = "x";
  public static final String JSON_GLM_NEG_X = "neg_x";
  public static final String JSON_GLM_FAMILY = "family";
  public static final String JSON_GLM_NORM = "norm";
  public static final String JSON_GLM_LAMBDA = "lambda";
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
  public static final String JSON_GLM_EXPAND_CAT = "expand_cat";

  public static final String JSON_ROWS = "rows";
  public static final String JSON_TIME = "time";
  public static final String JSON_COEFFICIENTS = "coefficients";

  protected final H2OHexKey _key = new H2OHexKey(JSON_KEY);
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(_key, JSON_GLM_Y);
  protected final IgnoreHexCols _x = new IgnoreHexCols(_key, _y, JSON_GLM_X);
  protected final IgnoreHexCols _negX = new IgnoreHexCols(_key, _y, JSON_GLM_NEG_X);


  protected final EnumArgument<Family> _family = new EnumArgument(JSON_GLM_FAMILY,Family.gaussian);
  protected final EnumArgument<Norm> _norm = new EnumArgument(JSON_GLM_NORM,Norm.NONE);

  protected final Real _lambda = new Real(JSON_GLM_LAMBDA, LSMSolver.DEFAULT_LAMBDA); // TODO I do not know the bounds

  protected final Real _lambda2 = new Real(JSON_GLM_LAMBDA_2, LSMSolver.DEFAULT_LAMBDA2);
  protected final Real _alpha = new Real(JSON_GLM_ALPHA, LSMSolver.DEFAULT_ALPHA, -1d, 1.8d);
  protected final Real _rho = new Real(JSON_GLM_RHO, LSMSolver.DEFAULT_RHO); // TODO I do not know the bounds

  protected final Int _maxIter = new Int(JSON_GLM_MAX_ITER, GLMSolver.DEFAULT_MAX_ITER, 1, 100);
  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);
  protected final Real _threshold = new Real(JSON_GLM_THRESHOLD,0.5d,0d,1d);
  protected final Real _case = new Real(JSON_GLM_CASE, 1.0); // TODO I do not know the bounds
  protected final EnumArgument<Link> _link = new EnumArgument(JSON_GLM_LINK,Link.familyDefault);
  protected final Int _xval = new Int(JSON_GLM_XVAL, 10, 1, Integer.MAX_VALUE);

  protected final Bool _expandCat = new Bool(JSON_GLM_EXPAND_CAT,false,"Expand categories");
  protected final Real _betaEps = new Real(JSON_GLM_BETA_EPS,GLMSolver.DEFAULT_BETA_EPS);

  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    if (arg == _family) {
      if (_family.value() != Family.binomial) {
        _case.disable("Only for family binomial");
        _weight.disable("Only for family binomial");
      }
    }
    if (arg == _norm) {
      switch (_norm.value()) {
        case NONE:
          _lambda.disable("Not available for this type of normalization");
          _lambda2.disable("Not available for this type of normalization");
          _alpha.disable("Not available for this type of normalization");
          _rho.disable("Not available for this type of normalization");
          break;
        case L1:
          _lambda2.disable("Not available for this type of normalization");
          break;
        case L2:
          _lambda2.disable("Not available for this type of normalization");
          _alpha.disable("Not available for this type of normalization");
          _rho.disable("Not available for this type of normalization");
          break;
        case ELASTIC:
      }
    }
  }

  public GLM() {
    _family.setRefreshOnChange();
    _norm.setRefreshOnChange();
    _negX._hideInQuery = true;
  }



  /** Returns an array of columns to use for GLM, the last of them being the
   * result column y.
   */
  private  int[] createColumns() {
    int y = _y.value();
    int[] cols = new int[_key.value()._cols.length];
    int[] x = _x.value();
    int[] negX = _negX.value();
    // get the X columns as 1
    for (int i : x)
      cols[i] = 1;
    // get the negX columns
    for (int i : negX) {
      if (cols[i] != 0)
        throw new Error("Column "+i+" has already been selected as X, cannot be also negX");
      cols[i] = -1;
    }
    // if no X were specified, create everyone that is not y or -1 as X
    int size = 1; // for the y column
    if (x.length == 0) {
      for (int i = 0; i < cols.length; ++i)
        if ((cols[i] == 0) && (i != y)) {
          cols[i] = 1;
          ++size;
        }
    } else {
      size += x.length;
    }
    int[] result = new int[size];
    int idx = 0;
    for (int i = 0; i < cols.length; ++i)
      if (cols[i] == 1)
        result[idx++] = i;
    result[idx++] = y;
    assert (idx == result.length);
    return result;
  }

  static JsonObject getCoefficients(int [] columnIds, ValueArray ary, double [] beta){
    JsonObject coefficients = new JsonObject();
    for( int i = 0; i < beta.length; ++i ) {
      String colName = (i == (beta.length - 1)) ? "Intercept" : ary._cols[columnIds[i]]._name;
      coefficients.addProperty(colName, beta[i]);
    }
    return coefficients;
  }

  static DecimalFormat dformat = new DecimalFormat("###.####");


  double [] getFamilyArgs(Family f){
    double [] res = null;
    if(f == Family.binomial) {
      res = new double []{1.0,1.0,0.5};
      res[GLMSolver.FAMILY_ARGS_CASE] = _case.value();
      res[GLMSolver.FAMILY_ARGS_WEIGHT] = _weight.value();
      res[GLMSolver.FAMILY_ARGS_DECISION_THRESHOLD] = _threshold.value();
    }
    return res;
  }

  GLMParams getGLMParams(){
    GLMParams res = new GLMParams();
    res._f = _family.value();
    res._l = _link.value();
    if (res._l == Link.familyDefault)
      res._l = res._f.defaultLink;
    res._expandCat = _expandCat.value();
    res._maxIter = _maxIter.value();
    res._betaEps = _betaEps.value();
    res._familyArgs = getFamilyArgs(res._f);
    return res;
  }

  LSMSolver getLSMSolver() {
    switch (_norm.value()) {
      case NONE:
        return LSMSolver.makeSolver();
      case L1:
        return LSMSolver.makeL1Solver(_lambda.value(), _rho.value(), _alpha.value());
      case L2:
        return LSMSolver.makeL2Solver(_lambda.value());
      case ELASTIC:
        return LSMSolver.makeElasticNetSolver(_lambda.value(), _lambda2.value(), _rho.value(), _alpha.value());
      default:
        throw new Error("Unexpected solver type");
    }
  }




  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _key.value();
      int Y = _y.value();
      int[] columns = createColumns();

      res.addProperty("key", ary._key.toString());
      res.addProperty("h2o", H2O.SELF.toString());

      GLMParams glmParams = getGLMParams();
      LSMSolver lsm = getLSMSolver();
      GLMSolver glm = new GLMSolver(lsm, glmParams);
      GLMModel m = glm.computeGLM(ary, columns, null);
      if(m._warnings != null){
        JsonArray warnings = new JsonArray();
        for(String w:m._warnings)warnings.add(new JsonPrimitive(w));
        res.add("warnings", warnings);
      }
      m.validateOn(ary, null);
      res.add("GLMModel", m.toJson());
      if (_xval.specified()) {
        int fold = _xval.value();
        JsonArray models = new JsonArray();
        for(GLMModel xm:glm.xvalidate(ary, columns, fold))
          models.add(xm.toJson());
        res.add("xval", models);
      }
      return Response.done(res).setBuilder("GLMModel.coefficients", new GLMCoeffBuilder(glmParams._l));
    } catch (Throwable t) {
      t.printStackTrace();
      return Response.error(t.getMessage());
    }
  }

  public class GLMCoeffBuilder extends ElementBuilder {

    final Link _link;


    @Override protected String objectToString(JsonObject obj, String contextName) {
      RString m = null;

      switch(_link){
      case identity:
        m = new RString("y = %equation");
        break;
      case logit:
        m = new RString("y = 1/(1 + Math.exp(%equation))");
        break;
      default:
        assert false;
        return "";
      }
      boolean first = true;
      StringBuilder bldr = new StringBuilder();
      for(Map.Entry<String,JsonElement> e:obj.entrySet()){

        double v = e.getValue().getAsDouble();
        if(v == 0)continue;
        if(!first)
          bldr.append(((v < 0)?" - ":" + ") + dformat.format(Math.abs(v)));
        else
          bldr.append(dformat.format(v));
        first = false;
        bldr.append("*x[" + e.getKey() + "]");
      }
      m.replace("equation",bldr.toString());
      return "<pre>"+m.toString()+"</pre>";
    }

    public GLMCoeffBuilder(Link link) {
      _link = link;
    }
  }

}
