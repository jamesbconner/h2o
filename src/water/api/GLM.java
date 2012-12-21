package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import hex.GLMSolver;
import hex.GLMSolver.*;
import hex.LSMSolver.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Properties;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class GLM extends Request {

//  enum GlmLink {
//    identity,
//    logit,
//    log,
//    inverse
//  }
//
//  public static final String JSON_GLM_Y = "y";
//  public static final String JSON_GLM_X = "x";
//  public static final String JSON_GLM_NEG_X = "neg_x";
//  public static final String JSON_GLM_FAMILY = "family";
//  public static final String JSON_GLM_NORM = "norm";
//  public static final String JSON_GLM_LAMBDA = "lambda";
//  public static final String JSON_GLM_LAMBDA_2 = "lambda_2";
//  public static final String JSON_GLM_RHO = "rho";
//  public static final String JSON_GLM_ALPHA = "alpha";
//  public static final String JSON_GLM_MAX_ITER = "max_iter";
//  public static final String JSON_GLM_BETA_EPS = "beta_eps";
//  public static final String JSON_GLM_WEIGHT = "weight";
//  public static final String JSON_GLM_THRESHOLD = "threshold";
//  public static final String JSON_GLM_XVAL = "xval";
//  public static final String JSON_GLM_CASE = "case";
//  public static final String JSON_GLM_LINK = "link";
//
//  public static final String JSON_ROWS = "rows";
//  public static final String JSON_TIME = "time";
//  public static final String JSON_COEFFICIENTS = "coefficients";
//
//  protected final H2OHexKey _key = new H2OHexKey(JSON_KEY);
//  protected final H2OHexKeyCol _y = new H2OHexKeyCol(_key, JSON_GLM_Y);
//  protected final IgnoreHexCols _x = new IgnoreHexCols(_key, _y, JSON_GLM_X);
//  protected final IgnoreHexCols _negX = new IgnoreHexCols(_key, _y, JSON_GLM_NEG_X);
//
//
//  protected final EnumArgument<Family> _family = new EnumArgument(JSON_GLM_FAMILY,Family.gaussian);
//  protected final EnumArgument<Norm> _norm = new EnumArgument(JSON_GLM_NORM,Norm.NO);
//
//  protected final Real _lambda = new Real(JSON_GLM_LAMBDA, 0.1); // TODO I do not know the bounds
//
//  protected final Int _maxIter = new Int(JSON_GLM_MAX_ITER, 1, 1, 100);
//  protected final Real _alpha = new Real(JSON_GLM_ALPHA, 1d, -1d, 1.8d);
//  protected final Real _rho = new Real(JSON_GLM_RHO, 0.01); // TODO I do not know the bounds
//  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);
//  protected final Real _threshold = new Real(JSON_GLM_THRESHOLD,0.5d,0d,1d);
//  protected final Real _case = new Real(JSON_GLM_CASE, 1.0); // TODO I do not know the bounds
//  protected final EnumArgument<Link> _link = new EnumArgument(JSON_GLM_LINK,Link.familyDefault);
//
//  protected final Int _xval = new Int(JSON_GLM_XVAL, 1, 1, Integer.MAX_VALUE);
//
//  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
//    if (arg == _family) {
//      if (_family.value() != Family.binomial) {
//        _case.disable("Only for family binomial");
//        _weight.disable("Only for family binomial");
//      }
//    }
//  }
//
//  public GLM() {
//    _family.refreshOnChange();
//  }
//
//
//
//  /** Returns an array of columns to use for GLM, the last of them being the
//   * result column y.
//   */
//  private  int[] createColumns() {
//    int y = _y.value();
//    int[] cols = new int[_key.value()._cols.length];
//    int[] x = _x.value();
//    int[] negX = _negX.value();
//    // get the X columns as 1
//    for (int i : x)
//      cols[i] = 1;
//    // get the negX columns
//    for (int i : negX) {
//      if (cols[i] != 0)
//        throw new Error("Column "+i+" has already been selected as X, cannot be also negX");
//      cols[i] = -1;
//    }
//    // if no X were specified, create everyone that is not y or -1 as X
//    int size = 1; // for the y column
//    if (x.length == 0) {
//      for (int i = 0; i < cols.length; ++i)
//        if ((cols[i] == 0) && (i != y)) {
//          cols[i] = 1;
//          ++size;
//        }
//    } else {
//      size += x.length;
//    }
//    int[] result = new int[size];
//    int idx = 0;
//    for (int i = 0; i < cols.length; ++i)
//      if (cols[i] == 1)
//        result[idx++] = i;
//    result[idx++] = y;
//    assert (idx == result.length);
//    return result;
//  }
//
//  static JsonObject getCoefficients(int [] columnIds, ValueArray ary, double [] beta){
//    JsonObject coefficients = new JsonObject();
//    for( int i = 0; i < beta.length; ++i ) {
//      String colName = (i == (beta.length - 1)) ? "Intercept" : ary._cols[columnIds[i]]._name;
//      coefficients.addProperty(colName, beta[i]);
//    }
//    return coefficients;
//  }
//
//  static DecimalFormat dformat = new DecimalFormat("###.####");
//
//
  @Override
  protected Response serve() {
    return null;
//    try {
//      JsonObject result = new JsonObject();
//      ValueArray va = _key.value();
//      int[] cols = createColumns();
//      Link link = _link.value();
//      Family family = _family.value();
//      if (link == Link.familyDefault)
//        link = family.defaultLink;
//
//      JsonObject jLsmParams = new JsonObject();
//      GLMSolver.GLM_Params glmParams = new GLMSolver.GLM_Params(family, link);
//      FamilyArgs fargs = null;
//      if (family == Family.binomial) {
//        double[] wt = new double[] {1.0, Math.sqrt(_weight.value()) };
//        fargs = new DGLM.BinomialArgs(_threshold.value(), _case.value(), wt);
//        jLsmParams.addProperty(JSON_GLM_WEIGHT, Arrays.toString(wt));
//      }
//
//      LSM_Params lsmParams = new LSM_Params(_norm.value(), _lambda.value(), _rho.value(), _alpha.value(), 1);
//      JsonObject jGlmParams = new JsonObject();
//      jGlmParams.addProperty(JSON_GLM_LINK, glmParams.link.toString());
//      jGlmParams.addProperty(JSON_GLM_FAMILY, glmParams.family.toString());
//      jGlmParams.addProperty(JSON_GLM_THRESHOLD, _threshold.value());
//
//      jLsmParams.addProperty(JSON_GLM_NORM, lsmParams.n.toString());
//      jLsmParams.addProperty(JSON_GLM_LAMBDA, lsmParams.lambda);
//      jLsmParams.addProperty(JSON_GLM_RHO, lsmParams.rho);
//      jLsmParams.addProperty(JSON_GLM_ALPHA, lsmParams.alpha);
//
//      long tStart = System.currentTimeMillis();
//
//      DGLM glm = new DGLM(glmParams, lsmParams, fargs);
//      GLMModel m = glm.trainOn(_key.value(), cols, null);
//
//      long deltaT = System.currentTimeMillis() - tStart;
//
//      if (m._warnings != null) {
//        JsonArray w = new JsonArray();
//        for (String s : m._warnings)
//          w.add(new JsonPrimitive(s));
//        result.add("warnings",w);
//      }
//
//      result.add("glmParams", jGlmParams);
//      result.add("lsmParams",jLsmParams);
//
//      result.addProperty(JSON_ROWS, cols.length); // WHY????
//      result.addProperty(JSON_TIME, deltaT);
//      result.add(JSON_COEFFICIENTS, getCoefficients(cols, va, m.beta()));
//
//
//      DGLM.GLMValidation val = (DGLM.GLMValidation)m.validateOn(va._key, null);
//      if(val != null){
//        JsonObject trainingValidation = new JsonObject();
//        trainingValidation.addProperty("DegreesOfFreedom", m.n() - 1);
////        trainingValidation.addProperty("ResidualDegreesOfFreedom", m.n() - X.length - 1);
//        trainingValidation.addProperty("ResidualDegreesOfFreedom", m.n() - va._cols.length -2); //TODO I am not sure if I have done this right
//        trainingValidation.addProperty("NullDeviance", dformat.format(val.nullDeviance()));
//        trainingValidation.addProperty("ResidualDeviance", dformat.format(val.resDeviance()));
//        //int k = X.length + 1;
//        int k = va._cols.length; // TODO I am not sure if I have done this right
//        trainingValidation.addProperty("AIC", dformat.format(2 * k + val.resDeviance()));
//        trainingValidation.addProperty("trainingSetErrorRate",dformat.format(val.err()));
//        result.add("trainingSetValidation", trainingValidation);
//        if(val instanceof DGLM.GLMBinomialValidation){
//          DGLM.GLMBinomialValidation bv = (DGLM.GLMBinomialValidation)val;
//          JsonObject errDetails = new JsonObject();
//          errDetails.addProperty("falsePositive", dformat.format(bv.fp()));
//          errDetails.addProperty("falseNegative", dformat.format(bv.fn()));
//          errDetails.addProperty("truePositive", dformat.format(bv.tp()));
//          errDetails.addProperty("trueNegative", dformat.format(bv.tn()));
//          JsonArray arr = new JsonArray();
//          for(int j = 0; j < bv.classes(); ++j){
//            JsonArray row = new JsonArray();
//            for(int kk = 0; kk < bv.classes();++kk)
//              row.add(new JsonPrimitive(bv.cm(j,kk)));
//            arr.add(row);
//          }
//          errDetails.add("cm", arr);
//          result.add("trainingErrorDetails", errDetails);
//        }
//      }
//      // Cross Validation
//      int xfactor = _xval.value();
//      if(xfactor <= 1)
//        return Response.done(result);
//      if(xfactor > m.n())xfactor = (int)m.n();
//      result.addProperty("xfactor", xfactor);
//      result.addProperty("threshold", _threshold.value());
//      Models.ModelValidation [] vals = Models.crossValidate(glm, xfactor, va, cols, 20);
//      if(vals[0] instanceof Models.BinaryClassifierValidation){
//        Models.BinaryClassifierValidation v = (Models.BinaryClassifierValidation)vals[0];
//        result.addProperty("trueNegative", dformat.format(v.tn()));
//        result.addProperty("trueNegativeVar", dformat.format(v.tnVar()));
//        result.addProperty("truePositive", dformat.format(v.tp()));
//        result.addProperty("truePositiveVar", dformat.format(v.tpVar()));
//        result.addProperty("falseNegative", dformat.format(v.fn()));
//        result.addProperty("falseNegativeVar", dformat.format(v.fnVar()));
//        result.addProperty("falsePositive", dformat.format(v.fp()));
//      }
//      // add individual models
//      if(vals[0] instanceof Models.ClassifierValidation){
//        JsonArray models = new JsonArray();
//        for(int i = 1; i < vals.length; ++i) {
//          JsonObject im = new JsonObject();
//          JsonArray arr = new JsonArray();
//          Models.ClassifierValidation v = (Models.ClassifierValidation)vals[i];
//          for(int j = 0; j < v.classes(); ++j){
//            JsonArray row = new JsonArray();
//            for(int k = 0; k < v.classes();++k)
//              row.add(new JsonPrimitive(v.cm(j,k)));
//            arr.add(row);
//          }
//          im.add("cm", arr);
//          models.add(im);
//        }
//        result.add("models", models);
//        result.addProperty("errRate", dformat.format(val.err()));
//      //res.addProperty("errRateVar", dformat.format(val.errVar()));
//      }
//      return Response.done(result);
//
//    } catch( DGLM.GLSMException e2 ) {
//      return Response.error("Unable to run the regression on this data: '"
//          + e2.getMessage() + "'");
//    }
  }


}
