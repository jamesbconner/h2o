package water.api;

import hex.*;
import hex.GLMSolver.*;
import hex.LSMSolver.Norm;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import water.*;
import water.web.RString;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

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
  public static final String JSON_GLM_MAX_ITER = "max_iter";
  public static final String JSON_GLM_BETA_EPS = "beta_eps";
  public static final String JSON_GLM_WEIGHT = "weight";
  public static final String JSON_GLM_XVAL = "xval";
  public static final String JSON_GLM_CASE = "case";
  public static final String JSON_GLM_LINK = "link";

  public static final String JSON_ROWS = "rows";
  public static final String JSON_TIME = "time";
  public static final String JSON_COEFFICIENTS = "coefficients";

  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(_key, JSON_GLM_Y);
  protected final HexColumnSelect _x = new HexNonConstantColumnSelect(JSON_GLM_X, _key, _y);
  protected final HexColumnSelect _negX = new HexColumnSelect(JSON_GLM_NEG_X, _key, _y);

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='GLM.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }


  protected final EnumArgument<Family> _family = new EnumArgument(JSON_GLM_FAMILY,Family.gaussian);
  protected final EnumArgument<Norm> _norm = new EnumArgument(JSON_GLM_NORM,Norm.ELASTIC);

  protected final Real _lambda1 = new Real(Constants.LAMBDA_1, LSMSolver.DEFAULT_LAMBDA); // TODO I do not know the bounds

  protected final Real _lambda2 = new Real(Constants.LAMBDA_2, LSMSolver.DEFAULT_LAMBDA2);
  protected final Real _alpha = new Real(Constants.ALPHA, LSMSolver.DEFAULT_ALPHA, -1d, 1.8d);
  protected final Real _rho = new Real(Constants.RHO, LSMSolver.DEFAULT_RHO); // TODO I do not know the bounds

  protected final Int _maxIter = new Int(JSON_GLM_MAX_ITER, GLMSolver.DEFAULT_MAX_ITER, 1, 1000000);
  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);
  protected final CaseSelect _case = new CaseSelect(_key,_y,JSON_GLM_CASE);
  protected final EnumArgument<Link> _link = new EnumArgument(JSON_GLM_LINK,Link.familyDefault);
  protected final Int _xval = new Int(JSON_GLM_XVAL, 10, 0, 1000000);

  protected final Real _betaEps = new Real(JSON_GLM_BETA_EPS,GLMSolver.DEFAULT_BETA_EPS);
  protected final RSeq _thresholds = new RSeq(Constants.DTHRESHOLDS, false, new NumberSequence("0:1:0.01", false, 0.01),false);

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
        _lambda1.disable("Not available for this type of normalization");
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
    BitSet cols = new BitSet();
    for( int i :    _x.value() ) cols.set  (i);
    for( int i : _negX.value() ) cols.clear(i);
    int[] res = new int[cols.cardinality()+1];
    int x=0;
    for( int i = cols.nextSetBit(0); i >= 0; i = cols.nextSetBit(i+1))
      res[x++] = i;
    res[x] = _y.value();
    return res;
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
      res = new double []{1.0,1.0};
      res[GLMSolver.FAMILY_ARGS_CASE] = _case.value();
      res[GLMSolver.FAMILY_ARGS_WEIGHT] = _weight.value();
    }
    return res;
  }

  GLMParams getGLMParams(){
    GLMParams res = new GLMParams();
    res._f = _family.value();
    res._l = _link.value();
    if( res._l == Link.familyDefault )
      res._l = res._f.defaultLink;
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
      return LSMSolver.makeL1Solver(_lambda1.value(), _rho.value(), _alpha.value());
    case L2:
      return LSMSolver.makeL2Solver(_lambda1.value());
    case ELASTIC:
      return LSMSolver.makeElasticNetSolver(_lambda1.value(), _lambda2.value(), _rho.value(), _alpha.value());
    default:
      throw new Error("Unexpected solver type");
    }
  }


  @Override protected Response serve() {
    try {
      JsonObject res = new JsonObject();
      ValueArray ary = _key.value();
      int[] columns = createColumns();

      res.addProperty("key", ary._key.toString());
      res.addProperty("h2o", H2O.SELF.toString());

      GLMParams glmParams = getGLMParams();
      LSMSolver lsm = getLSMSolver();
      GLMSolver glm = new GLMSolver(lsm, glmParams);
      GLMModel m = glm.computeGLM(ary, columns, null);
      if( m.is_solved() ) {     // Solved at all?
        if( _xval.specified() && _xval.value() > 0 ) // ... and x-validate
          glm.xvalidate(m,ary,columns,_xval.value(),_thresholds.value().arr);
        else
          m.validateOn(ary, null,_thresholds.value().arr);// Validate...
      }
      m.store();
      // Convert to JSON
      res.add("GLMModel", m.toJson());

      // Display HTML setup
      Response r = Response.done(res);
      r.setBuilder(""/*top-level do-it-all builder*/,new GLMBuilder(m));
      return r;
    }catch(GLMException e){
      return Response.error(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
      return Response.error(t.getMessage());
    }
  }


  static class GLMBuilder extends ObjectBuilder {
    final GLMModel _m;
    GLMBuilder( GLMModel m) { _m=m; }
    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();;
      modelHTML(_m,json.get("GLMModel").getAsJsonObject(),sb);
      return sb.toString();
    }

    private static void modelHTML( GLMModel m, JsonObject json, StringBuilder sb ) {
      RString R = new RString(
          "<div class='alert %succ'>GLM on data <a href='/Inspect.html?"+KEY+"=%key'>%key</a>. %iterations iterations computed in %time. %warnings</div>" +
          "<h4>GLM Parameters</h4>" +
          " %GLMParams %LSMParams" +
          "<h4>Equation: </h4>" +
          "<div><code>%modelSrc</code></div>"+
          "<h4>Coefficients</h4>" +
          "<div>%coefficients</div>");

      // Warnings

      if( m._warnings != null ) {
        StringBuilder wsb = new StringBuilder();
        for( String s : m._warnings )
          wsb.append(s).append("<br>");
        R.replace("warnings",wsb);
        R.replace("succ","alert-warning");
      } else
        R.replace("succ","alert-success");

      // Basic model stuff
      R.replace("key",m._dataset);
      R.replace("time",PrettyPrint.msecs(m._time,true));
      R.replace("iterations",m._iterations);
      R.replace("GLMParams",glmParamsHTML(m));
      R.replace("LSMParams",lsmParamsHTML(m));

      // Pretty equations
      if( m.is_solved() ) {
        JsonObject coefs = json.get("coefficients").getAsJsonObject();
        R.replace("modelSrc",equationHTML(m,coefs));
        R.replace("coefficients",coefsHTML(coefs));
      }
      sb.append(R);

      // Validation / scoring
      validationHTML(m._vals,sb);
    }

    private static final String LAMBDA1 = "&lambda;<sub>1</sub>";
    private static final String LAMBDA2 = "&lambda;<sub>2</sub>";
    private static final String RHO     = "&rho;";
    private static final String ALPHA   = "&alpha;";
    private static final String EPSILON = "&epsilon;<sub>&beta;</sub>";

    private static final DecimalFormat DFORMAT = new DecimalFormat("###.####");
    private static final String dformat( double d ) {
      return Double.isNaN(d) ? "NaN" : DFORMAT.format(d);
    }

    private static void parm( StringBuilder sb, String x, Object... y ) {
      sb.append("<span><b>").append(x).append(": </b>").append(y[0]).append("</span> ");
    }

    private static String glmParamsHTML( GLMModel m ) {
      StringBuilder sb = new StringBuilder();
      GLMParams glmp = m._glmParams;
      parm(sb,"family",glmp._f);
      parm(sb,"link",glmp._l);
      parm(sb,EPSILON,glmp._betaEps);
      double[] fa = glmp._familyArgs;
      if( glmp._f == GLMSolver.Family.binomial ) {
        if( !Double.isNaN(fa[GLMSolver.FAMILY_ARGS_CASE]) ) {
          parm(sb,"case",(int)fa[GLMSolver.FAMILY_ARGS_WEIGHT]);
          if( fa[GLMSolver.FAMILY_ARGS_WEIGHT] != 1.0 )
            parm(sb,"weight",fa[GLMSolver.FAMILY_ARGS_WEIGHT]);
        }
      }
      return sb.toString();
    }

    private static String lsmParamsHTML( GLMModel m ) {
      StringBuilder sb = new StringBuilder();
      LSMSolver lsm = m._solver;
      switch( lsm._penalty ) {
      case NONE: break;
      case L1:
        parm(sb,"penalty","l1");
        parm(sb,LAMBDA1,lsm._lambda);
        parm(sb,RHO    ,lsm._rho);
        parm(sb,ALPHA  ,lsm._alpha);
        break;
      case L2:
        parm(sb,"penalty","l2");
        parm(sb,LAMBDA1,lsm._lambda);
        break;
      case ELASTIC:
        parm(sb,"penalty","l1+l2");
        parm(sb,LAMBDA1,lsm._lambda);
        parm(sb,LAMBDA2,lsm._lambda2);
        parm(sb,RHO    ,lsm._rho);
        parm(sb,ALPHA  ,lsm._alpha);
        break;
      }
      return sb.toString();
    }

    // Pretty equations
    private static String equationHTML( GLMModel m, JsonObject coefs ) {
      RString eq = null;
      switch( m._glmParams._l ) {
      case identity: eq = new RString("y = %equation");   break;
      case logit:    eq = new RString("y = 1/(1 + Math.exp(-(%equation)))");  break;
      default:       eq = new RString("equation display not implemented"); break;
      }
      StringBuilder sb = new StringBuilder();
      for( Entry<String,JsonElement> e : coefs.entrySet() ) {
        if( e.getKey().equals("Intercept") ) continue;
        double v = e.getValue().getAsDouble();
        if( v == 0 ) continue;
        sb.append(dformat(v)).append("*x[").append(e.getKey()).append("] + ");
      }
      sb.append(coefs.get("Intercept").getAsDouble());
      eq.replace("equation",sb.toString());
      return eq.toString();
    }

    private static String coefsHTML( JsonObject coefs ) {
      StringBuilder sb = new StringBuilder();
      sb.append("<table class='table table-bordered table-condensed'>");
      sb.append("<tr>");
      for( Entry<String,JsonElement> e : coefs.entrySet() )
        sb.append("<th>").append(e.getKey()).append("</th>");
      sb.append("</tr>");
      sb.append("<tr>");
      for( Entry<String,JsonElement> e : coefs.entrySet() )
        sb.append("<td>").append(e.getValue().getAsDouble()).append("</td>");
      sb.append("</tr>");
      sb.append("</table>");
      return sb.toString();
    }

    private static void validationHTML( GLMValidation[] vals, StringBuilder sb) {
      if( vals == null || vals.length == 0 ) return;
      sb.append("<h4>Validations</h4>");

      for( GLMValidation val : vals ) {
        RString R = new RString("<table class='table table-striped table-bordered table-condensed'>"
            + "<tr><th>Degrees of freedom:</th><td>%DegreesOfFreedom total (i.e. Null);  %ResidualDegreesOfFreedom Residual</td></tr>"
            + "<tr><th>Null Deviance</th><td>%nullDev</td></tr>"
            + "<tr><th>Residual Deviance</th><td>%resDev</td></tr>"
            + "<tr><th>AIC</th><td>%AIC</td></tr>"
            + "<tr><th>Training Error Rate Avg</th><td>%err</td></tr>"
            +"%CM"
            + "</table>");
        RString R2 = new RString(
            "<tr><th>AUC</th><td>%AUC</td></tr>"
            + "<tr><th>Best Threshold</th><td>%threshold</td></tr>");

        R.replace("DegreesOfFreedom",val._n-1);
        R.replace("ResidualDegreesOfFreedom",val._dof);
        R.replace("nullDev",val._nullDeviance);
        R.replace("resDev",val._deviance);
        R.replace("AIC", dformat(val.AIC()));
        R.replace("err",val.err());

        if(val._cm != null){
          R2.replace("AUC", dformat(val.AUC()));
          R2.replace("threshold", dformat(val.bestThreshold()));
          R.replace("CM",R2);
        }
        sb.append(R);
        confusionHTML(val.bestCM(),sb);
        if(val instanceof GLMXValidation){
          GLMXValidation xval = (GLMXValidation)val;
          int nclasses = 2;
          sb.append("<table class='table table-bordered table-condensed'>");
          if(xval._cm != null){
            sb.append("<tr><th>Model</th><th>Best Threshold</th><th>AUC</th>");
            for(int c = 0; c < nclasses; ++c)
              sb.append("<th>Err(" + c + ")</th>");
            sb.append("</tr>");
            // Display all completed models
            int i=0;
            for(GLMModel xm:xval.models()){
              String mname = "Model " + i++;
              sb.append("<tr>");
              try {
                sb.append("<td>" + "<a href='Inspect.html?"+KEY+"="+URLEncoder.encode(xm.key().toString(),"UTF-8")+"'>" + mname + "</a></td>");
              } catch( UnsupportedEncodingException e1 ) {
                throw new Error(e1);
              }
              sb.append("<td>" + dformat(xm._vals[0].bestThreshold()) + "</td>");
              sb.append("<td>" + dformat(xm._vals[0].AUC()) + "</td>");
              for(double e:xm._vals[0].classError())
                sb.append("<td>" + dformat(e) + "</td>");
              sb.append("</tr>");
            }
          } else {
            sb.append("<tr><th>Model</th><th>Error</th>");
            sb.append("</tr>");
            // Display all completed models
            int i=0;
            for(GLMModel xm:xval.models()){
              String mname = "Model " + i++;
              sb.append("<tr>");
              try {
                sb.append("<td>" + "<a href='Inspect.html?"+KEY+"="+URLEncoder.encode(xm.key().toString(),"UTF-8")+"'>" + mname + "</a></td>");
              } catch( UnsupportedEncodingException e1 ) {
                throw new Error(e1);
              }
              sb.append("<td>" + xm._vals[0]._err + "</td>");
              sb.append("</tr>");
            }
          }
          sb.append("</table>");
        }
      }
    }

    private static void cmRow( StringBuilder sb, String hd, double c0, double c1, double cerr ) {
      sb.append("<tr><th>").append(hd).append("</th><td>");
      if( !Double.isNaN(c0  )) sb.append( dformat(c0  ));
      sb.append("</td><td>");
      if( !Double.isNaN(c1  )) sb.append( dformat(c1  ));
      sb.append("</td><td>");
      if( !Double.isNaN(cerr)) sb.append( dformat(cerr));
      sb.append("</td></tr>");
    }

    private static void confusionHTML( GLMSolver.ConfusionMatrix cm, StringBuilder sb) {
      if( cm == null ) return;
      sb.append("<table class='table table-bordered table-condensed'>");
      sb.append("<tr><th>Actual / Predicted</th><th>false</th><th>true</th><th>Err</th></tr>");
      double err0 = cm._arr[0][1]/(double)(cm._arr[0][0]+cm._arr[0][1]);
      cmRow(sb,"false",cm._arr[0][0],cm._arr[0][1],err0);
      double err1 = cm._arr[1][0]/(double)(cm._arr[1][0]+cm._arr[1][1]);
      cmRow(sb,"true ",cm._arr[1][0],cm._arr[1][1],err1);
      double err2 = cm._arr[1][0]/(double)(cm._arr[0][0]+cm._arr[1][0]);
      double err3 = cm._arr[0][1]/(double)(cm._arr[0][1]+cm._arr[1][1]);
      cmRow(sb,"Err ",err2,err3,cm.err());
      sb.append("</table>");
    }
  }
}
