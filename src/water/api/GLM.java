package water.api;

import com.google.gson.*;
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

  protected final H2OHexKey _key = new H2OHexKey(KEY);
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

  double [] getFamilyArgs(Family f){
    double [] res = null;
    if( f == Family.binomial ) {
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
    if( res._l == Link.familyDefault )
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
      if( m.is_solved() ) m.validateOn(ary, null);
      res.add("GLMModel", m.toJson());

      if( m.is_solved() && _xval.specified() ) {
        int fold = _xval.value();
        JsonArray models = new JsonArray();
        for( GLMModel xm:glm.xvalidate(ary, columns, fold) )
          models.add(xm.toJson());
        res.add("xval", models);
      }
      return display(res);

    } catch (Throwable t) {
      t.printStackTrace();
      return Response.error(t.getMessage());
    }
  }

  // Add on all the builders
  private Response display( JsonObject res ) {
    Response r = Response.done(res);
    r.setBuilder("key", new KeyLinkElementBuilder());
    r.setBuilder("h2o", new HideBuilder());
    r.setBuilder("GLMModel", new NoCaptionObjectBuilder());
    r.setBuilder("GLMModel.isDone", new HideBuilder());
    r.setBuilder("GLMModel.dataset", new HideBuilder());
    r.setBuilder("GLMModel.coefficients", new GLMCoeffBuilder(Link.logit));
    r.setBuilder("GLMModel.LSMParams", new LSMParamsBuilder());
    r.setBuilder("GLMModel.GLMParams", new GLMParamsBuilder());
    r.setBuilder("GLMModel.warnings", new WarningsBuilder());
    return r;
  }  

  static DecimalFormat dformat = new DecimalFormat("###.#####");
  public class GLMCoeffBuilder extends ElementBuilder {
    final Link _link;
    public GLMCoeffBuilder(Link link) { _link = link; }
    @Override public String build(Response response, JsonElement elem, String contextName) {
      JsonObject obj = (JsonObject)elem;
      RString m = null;
      switch(_link) {
      case identity: m = new RString("y = %equation");   break;
      case logit:    m = new RString("y = 1/(1 + Math.exp(-(%equation)))");  break;
      default:       assert false;  return "";
      }
      StringBuilder bldr = new StringBuilder();
      for( Map.Entry<String,JsonElement> e : obj.entrySet() ) {
        if( e.getKey().equals("Intercept") ) continue;
        double v = e.getValue().getAsDouble();
        if( v == 0 ) continue;
        bldr.append(dformat.format(v)).append("*x[" + e.getKey() + "]").append(" + ");
      }
      bldr.append(obj.get("Intercept").getAsDouble());
      m.replace("equation",bldr.toString());
      return build("<pre>"+m.toString()+"</pre>","equation");
    }
  }

  private static double doubleOrNaN( JsonObject obj, String elem ) {
    JsonElement e = obj.get(elem);
    return e==null ? Double.NaN : e.getAsDouble();
  }

  public class LSMParamsBuilder extends NoCaptionObjectBuilder {
    private void parm( StringBuilder sb, String name, double d ) { parm(sb,name,Double.toString(d)); }
    private void parm( StringBuilder sb, String name, String s ) {
      sb.append("<b>"+name+"</b> ="+s+"&nbsp;&nbsp;&nbsp;");
    }
    @Override public String build(Response response, JsonElement elem, String contextName) {
      JsonObject obj = (JsonObject)elem;
      StringBuilder sb = new StringBuilder();
      String pen = obj.get("penalty").getAsString();
      double lambda = doubleOrNaN(obj,"lambda" );
      double lambda2= doubleOrNaN(obj,"lambda2");
      double rho    = doubleOrNaN(obj,"rho"    );
      double alpha  = doubleOrNaN(obj,"alpha"  );
      parm(sb,"Normalization Strategy",pen);
      if( pen.equals("none") ) {
      } else if( pen.equals("L1") ) {
        parm(sb,"Lambda", lambda);
        parm(sb,"rho",rho);
        parm(sb,"alpha",alpha);
      } else if( pen.equals("L2") ) {
        parm(sb,"Lambda",lambda);
      } else if( pen.equals("L1 + L2") ) {
        parm(sb,"Lambda",lambda);
        parm(sb,"rho",rho);
        parm(sb,"alpha",alpha);
        parm(sb,"Lambda2",lambda2);
      }
      sb.append("<br>");
      return sb.toString();
    }
  }

  public class GLMParamsBuilder extends NoCaptionObjectBuilder {
    private void parm( StringBuilder sb, String name, double d ) { parm(sb,name,Double.toString(d)); }
    private void parm( StringBuilder sb, String name, String s ) {
      sb.append("<b>"+name+"</b> ="+s+"&nbsp;&nbsp;&nbsp;");
    }
    @Override public String build(Response response, JsonElement elem, String contextName) {
      JsonObject obj = (JsonObject)elem;
      StringBuilder sb = new StringBuilder();
      parm(sb,"family",obj.get("family").getAsString());
      parm(sb,"link",obj.get("link").getAsString());
      parm(sb,"betaEps",obj.get("betaEps").getAsDouble());
      parm(sb,"maxIter",obj.get("maxIter").getAsDouble());
      parm(sb,"threshold",obj.get("threshold").getAsDouble());
      // Not displaying caseVal until it gets debugged right
      obj.get("caseVal").getAsDouble();
      obj.get("weight").getAsDouble();

      sb.append("<br>");
      return sb.toString();
    }
  }

  public class WarningsBuilder extends ArrayBuilder {
    public String build(Response response, JsonArray array, String contextName) {
      if( array.size()==0 ) return ""; // No title or 'nuttin
      return super.build(response,array,contextName);
    }    
  }

  // Feed this horrible json straight in to more rapidly turn around html debugging
  @Override protected Response serve_debug() {
    JsonObject res = (JsonObject)new JsonParser().parse(JUNK);
    return display(res);
  }

  static final String JUNK= "{\"key\":\"cars.csv.hex\",\"h2o\":\"/192.168.1.55:54321\",\"GLMModel\":{\"time\":171,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":1.1,\"power (hp)\":2.2,\"weight (lb)\":0.0,\"0-60 mph (s)\":0.0,\"year\":0.0,\"Intercept\":-14.269496989557405},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"nrows\":400,\"dof\":393,\"resDev\":5.080729189823018E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",400,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",400,0,0.0]]}]},\"xval\":[{\"time\":47,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":0.0,\"power (hp)\":0.0,\"weight (lb)\":0.0,\"0-60 mph (s)\":0.0,\"year\":0.0,\"Intercept\":-14.168984028783525},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=0complement=true)\",\"nrows\":360,\"dof\":353,\"resDev\":5.056159504729022E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",360,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",360,0,0.0]]}]},{\"time\":31,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":-8.228925975925305E-5,\"power (hp)\":4.5422785697973364E-5,\"weight (lb)\":0.0,\"0-60 mph (s)\":0.0,\"year\":0.0,\"Intercept\":-14.156244312461233},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=1complement=true)\",\"nrows\":359,\"dof\":352,\"resDev\":5.04989902234529E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",359,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",359,0,0.0]]}]},{\"time\":31,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":-1.430784564257528E-4,\"power (hp)\":0.0,\"weight (lb)\":0.0,\"0-60 mph (s)\":0.0,\"year\":0.0,\"Intercept\":-14.128655739573883},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=2complement=true)\",\"nrows\":359,\"dof\":352,\"resDev\":5.105673411408031E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",359,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",359,0,0.0]]}]},{\"time\":16,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":0.0,\"power (hp)\":0.001881180457588478,\"weight (lb)\":-5.6235295299388565E-5,\"0-60 mph (s)\":0.010631344527047145,\"year\":0.01648784032289129,\"Intercept\":-15.615126790137586},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=3complement=true)\",\"nrows\":360,\"dof\":353,\"resDev\":5.066194232876015E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",360,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",360,0,0.0]]}]},{\"time\":16,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":0.0,\"power (hp)\":0.0,\"weight (lb)\":-4.002087280111213E-6,\"0-60 mph (s)\":0.0,\"year\":0.01162473765384227,\"Intercept\":-15.052477065356488},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=4complement=true)\",\"nrows\":359,\"dof\":352,\"resDev\":4.97981385233808E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",359,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",359,0,0.0]]}]},{\"time\":31,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":0.0,\"power (hp)\":5.504302404714807E-4,\"weight (lb)\":0.0,\"0-60 mph (s)\":0.00810503982221933,\"year\":0.00854363964058845,\"Intercept\":-15.006182535290819},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=5complement=true)\",\"nrows\":360,\"dof\":353,\"resDev\":5.030786124969234E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",360,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",360,0,0.0]]}]},{\"time\":16,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":-2.7570227729564402E-5,\"power (hp)\":0.0011407022533520697,\"weight (lb)\":-1.1242627273864829E-5,\"0-60 mph (s)\":0.0,\"year\":0.0026874297123769624,\"Intercept\":-14.462803572212145},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=6complement=true)\",\"nrows\":360,\"dof\":353,\"resDev\":5.014566796191885E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",360,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",360,0,0.0]]}]},{\"time\":31,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":0.0,\"power (hp)\":0.0,\"weight (lb)\":0.0,\"0-60 mph (s)\":0.0,\"year\":-0.010485169450336021,\"Intercept\":-13.380637144596069},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=7complement=true)\",\"nrows\":362,\"dof\":355,\"resDev\":5.054433419276856E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",362,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",362,0,0.0]]}]},{\"time\":31,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":0.0,\"power (hp)\":0.0,\"weight (lb)\":5.367326813241483E-6,\"0-60 mph (s)\":0.004204034380553801,\"year\":-0.005054697435203621,\"Intercept\":-13.863863451418654},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=8complement=true)\",\"nrows\":361,\"dof\":354,\"resDev\":5.086187013942853E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",361,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",361,0,0.0]]}]},{\"time\":16,\"isDone\":true,\"dataset\":\"cars.csv.hex\",\"coefficients\":{\"displacement (cc)\":0.0,\"power (hp)\":0.0,\"weight (lb)\":0.0,\"0-60 mph (s)\":0.0,\"year\":-0.011620424177219849,\"Intercept\":-13.292627124298592},\"LSMParams\":{\"penalty\":\"L1 + L2\",\"lambda\":1.0E-5,\"lambda2\":1.0E-8,\"rho\":0.01,\"alpha\":1.0},\"GLMParams\":{\"family\":\"binomial\",\"link\":\"logit\",\"betaEps\":1.0E-4,\"maxIter\":50,\"caseVal\":1.0,\"weight\":1.0,\"threshold\":0.5},\"iterations\":51,\"validations\":[{\"dataset\":\"cars.csv.hex\",\"sampling\":\"Sampling(step=10,offset=9complement=true)\",\"nrows\":360,\"dof\":353,\"resDev\":5.032894829266778E-4,\"nullDev\":0.0,\"err\":0.0,\"cm\":[[\"Actual / Predicted\",\"class 0\",\"class 1\",\"Error\"],[\"class 0\",360,0,0.0],[\"class 1\",0,0,NaN],[\"Totals\",360,0,0.0]]}]}]}";
}
