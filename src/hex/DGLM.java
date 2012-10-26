package hex;

import hex.DLSM.LSMTask;
import hex.DLSM.LSM_Params;
import hex.RowVecTask.Sampling;

import java.util.ArrayList;
import java.util.Arrays;

import water.DRemoteTask;
import water.ValueArray;


/**
 * Distributed General Linear Model solver.
 *
 * Implemented as Iterative Reweighted LSM fitting problem. Calls DLSM iteratively until solution is found.
 *
 * Goal is to have distributed version of glm from R with few enhancements (L1 and L2 norm).
 *
 * Current Limitations:
 *   * only gaussian, binomial and poisson family supported at the moment.
 *   * limitations of underlying DLSM apply here as well
 *
 * Implemented by extending LSMTask (by IRLSMTask) to transform response variable and to apply weights in flight).
 *
 * @author tomasnykodym
 *
 */
public class DGLM {

  static final double DEFAULT_EPS = 1e-5;
  public final static int MAX_ITER = 50;

  public static enum Link {
    identity(0),
    logit(0),
    log(0),
//    probit(0),
//    cauchit(0),
//    cloglog(0),
//    sqrt(0),
//    inverse(1.0),
//    oneOverMu2(0);
    ;
    public final double defaultBeta;

    Link(double b){defaultBeta = b;}
  }

  // supported families
  public static enum Family {
    gaussian(Link.identity),
    binomial(Link.logit),
    poisson(Link.log),
    //gamma(Link.inverse);
    ;
    public final Link defaultLink;

    Family(Link l){defaultLink = l;}
  }

  // supported norms (penalty functions)
  public static enum Norm {
    NONE, // standard regression without any regularization
    L1,   // LASSO
    L2;   // ridge regression
  }

  /**
   * Per family variance computation
   *
   * @param family
   * @param mu
   * @return
   */
  public static double variance(Family family, double mu){
    switch(family){
    case gaussian:
      return 1;
    case binomial:
      assert 0 <= mu && mu <= 1;
      return mu*(1-mu);
    case poisson:
      return mu;
//    case gamma:
//      return mu*mu;
    default:
      throw new Error("unknown family Id " + family);
    }
  }


  // helper function
  static double y_log_y(double y, double mu){
    mu = Math.max(Double.MIN_NORMAL, mu);
    return (y != 0) ? (y * Math.log(y/mu)) : 0;
  }

  /**
   * Per family deviance computation.
   *
   * @param family
   * @param yr
   * @param ym
   * @return
   */
  public static double deviance(Family family, double yr, double ym){
    switch(family){
    case gaussian:
      return (yr - ym)*(yr - ym);
    case binomial:
      return 2*((y_log_y(yr, ym)) + y_log_y(1-yr, 1-ym));
      //return -2*(yr * ym - Math.log(1 + Math.exp(ym)));
    case poisson:
      //ym = Math.exp(ym);
      if(yr == 0)return 2*ym;
      return 2*((yr * Math.log(yr/ym)) - (yr - ym));
//    case gamma:
//      if(yr == 0)return -2;
//      return -2*(Math.log(yr/ym) - (yr - ym)/ym);
    default:
      throw new Error("unknown family Id " + family);
    }
  }

  /**
   * Link function computation.
   *
   * @param linkFunction
   * @param x
   * @return
   */
  public static double link(Link linkFunction, double x){
   switch(linkFunction){
     case identity:
       return x;
     case logit:
       assert 0 <= x && x <= 1;
       return Math.log(x/(1 - x));
     case log:
       return Math.log(x);
//     case inverse:
//       return 1/x;
     default:
       throw new Error("unsupported link function id  " + linkFunction);
   }
  }

  /**
   * Link function inverse computation.
   *
   * @param linkFunction
   * @param x
   * @return
   */
  public static double linkInv(Link linkFunction, double x){
    switch(linkFunction){
      case identity:
        return x;
      case logit:
        return 1.0 / (Math.exp(-x) + 1.0);
      case log:
        return Math.exp(x);
//      case inverse:
//        return 1/x;
      default:
        throw new Error("unexpected link function id  " + linkFunction);
    }
   }

  /**
   * Link function derivative computation.
   *
   * @param linkFunction
   * @param x
   * @return
   */
  public static double linkDeriv(Link linkFunction, double x){
    switch(linkFunction){
      case identity:
        return 1;
      case logit:
        if( x == 1 || x == 0 ) return MAX_SQRT;
        return 1 / (x * (1 - x));
      case log:
        return (x == 0)?MAX_SQRT:1/x;
//      case inverse:
//        return -1/(x*x);
      default:
        throw new Error("unexpected link function id  " + linkFunction);
    }
   }


  /**
   * Additional per-family args to glm.
   * @author tomasnykodym
   *
   */
  public static abstract class FamilyArgs{}

  /**
   *  Args for binomial family
   * @author tomasnykodym
   *
   */
  public static class BinomialArgs extends FamilyArgs {
    double _threshold = 0.5; // decision threshold for classification/validation
    double _case = 1.0; // value to be mapped to 1 (en eeverything else to 0).

    public BinomialArgs(double t, double c){
      _threshold = t;
      _case = c;
    }
  }

  /**
   * Paramters for GLM.
   * @author tomasnykodym
   *
   */
  public static class GLM_Params{
    double beta_eps = DGLM.DEFAULT_EPS; // precision level for beta
    public Family family = Family.gaussian;
    public Link link = Link.identity;
    DataPreprocessing preprocessing;

    public GLM_Params(Family f, Link l){
      this(f,l,DEFAULT_EPS,DataPreprocessing.AUTO);
    }
    public GLM_Params(Family f, Link l, double beps, DataPreprocessing p){
      family = f;
      link = l;
      beta_eps = beps;
      preprocessing = p;
    }
  }

  public static class GLSMException extends RuntimeException {
    public GLSMException(String msg) {
      super(msg);
    }
  }

  private static final double MAX_SQRT = Math.sqrt(Double.MAX_VALUE);

  Sampling         _sampling;
  /**
   *  Class for holding of validation info of given glm model.
   *
   * @author tomasnykodym
   *
   */
  public static class GLM_Validation implements Cloneable {
    GLM_Model _m;
    long      _n;
    double    _resDev;
    double    _nullDev;
    double    _errMean;
    double    _errVar;
    int       _t;

    public GLM_Validation(GLM_Model m, double err, double resDev, double nullDev, long n) {
      _m = m;
      _n = n;
      _errMean = err;
      _resDev = resDev;
      _nullDev = nullDev;
      _t = 1;
    }

    public GLM_Validation(GLM_Validation other) {
      _m = other._m;
      _n = other._n;
      _t = other._t;
      _errMean = other._errMean;
      _errVar = other._errVar;
      _t = other._t;
      _nullDev = other._nullDev;
      _resDev = other._resDev;
    }

    public void aggregate(GLM_Validation other) {
      // recursive avg formula
      _n += other._n;
      ++_t;
      _errMean = (_t - 1.0) / _t * _errMean + 1.0 / _t * other._errMean;
      // recursive variance formula
      double newVar = (other._errMean - _errMean);
      _errVar = ((_t - 1.0) / _t) * _errVar + (1.0 / (_t - 1)) * newVar
          * newVar;
    }

    public long n() {
      return _m.n;
    }

    public double errMean() {
      return _errMean;
    }

    public double errVar() {
      return _errVar;
    }

    public double nullDeviance() {
      return _nullDev;
    }

    public double residualDeviance() {
      return _resDev;
    }

    public GLM_Model m() {
      return _m;
    }

    @Override
    public GLM_Validation clone() {
      return new GLM_Validation(this);
    }
  }

  /**
   * Binomial validation adds confusion matrix (which is 2 by 2 in binomial case).
   * @author tomasnykodym
   *
   */
  public static class BinomialValidation extends GLM_Validation {
    static long sum(long[][] cm) {
      long res = 0;
      for( int i = 0; i < cm.length; ++i )
        for( int j = 0; j < cm[i].length; ++j )
          res += cm[i][j];
      return res;
    }

    static long sum_diag(long[][] cm) {
      long res = 0;
      for( int i = 0; i < cm.length; ++i )
        res += cm[i][i];
      return res;
    }

    long[][]   _cm;
    double[][] _cmMean;
    double[][] _cmVar;

    public BinomialValidation(GLM_Model m, long[][] cm, long n,
        double nullDeviance, double residualDeviance) {
      super(m, (sum(cm) - sum_diag(cm)) / (double) sum(cm),residualDeviance,nullDeviance, n);

      _cm = cm;
      _cmMean = new double[_cm.length][_cm.length];
      _cmVar = new double[_cm.length][_cm.length];
      double dn = 1.0 / Math.max(1, _n);
      for( int i = 0; i < _cmMean.length; ++i )
        for( int j = 0; j < _cmMean.length; ++j )
          _cmMean[i][j] = _cm[i][j] * dn;
    }

    public BinomialValidation(BinomialValidation other) {
      super(other);
      _cm = other._cm.clone();
      _cmMean = other._cmMean.clone();
      _cmVar = other._cmVar.clone();
    }

    @Override
    public void aggregate(GLM_Validation g) {
      super.aggregate(g);
      BinomialValidation other = (BinomialValidation) g;
      for( int i = 0; i < _cmMean.length; ++i )
        for( int j = 0; j < _cmMean.length; ++j ) {
          _cmMean[i][j] = (_t - 1.0) / _t * _cmMean[i][j] + 1.0 / _t
              * other._cmMean[i][j];
          double newVar = (other._cmMean[i][j] - _cmMean[i][j]);
          _cmVar[i][j] = ((_t - 1.0) / _t) * _cmVar[i][j] + (1.0 / (_t - 1))
              * newVar * newVar;
        }
    }

    public long cm(int i, int j) {
      return _cm[i][j];
    }

    public double fpMean() {
      return _cmMean[1][0];
    }

    public double fpVar() {
      return _cmVar[1][0];
    }

    public double fnMean() {
      return _cmMean[0][1];
    }

    public double fnVar() {
      return _cmVar[0][1];
    }

    public double tpMean() {
      return _cmMean[0][0];
    }

    public double tpVar() {
      return _cmVar[0][0];
    }

    public double tnMean() {
      return _cmMean[1][1];
    }

    public double tnVar() {
      return _cmVar[1][1];
    }

    @Override
    public BinomialValidation clone() {
      return new BinomialValidation(this);
    }
  }

  /**
   * Holds info about fitted glm model.
   *
   * @author tomasnykodym
   *
   */
  public static class GLM_Model {
    GLM_Params glmParams;
    LSM_Params lsmParams;
    double nullDeviance;
    double residualDeviance;
    double err;
    public double[] beta;
    public int [] colIds;
    int[]                 preprocessing;
    public String[]       warnings;
    public long     n;
    public GLM_Model(){}
    public GLM_Model(long n, double[] beta, int[] p, GLM_Params gp, LSM_Params lp) {
      this(n, beta, p, gp, lp, null);
    }
    public GLM_Model(long n, double[] beta, int[] p,GLM_Params gp, LSM_Params lp,
        String[] warnings) {
      this.n = n;
      this.beta = beta;
      this.warnings = warnings;
      preprocessing = p;
      glmParams = gp;
      lsmParams = lp;
    }
    public double apply(double[] x) {
      double res = 0.0;
      for( int i = 0; i < x.length; ++i )
        res += beta[i] * x[i];
      if( lsmParams.constant != 0 ) res += lsmParams.constant * beta[x.length];
      return res;
    }
    public GLM_Validation validateOn(ValueArray ary, Sampling s) {
      //double ymu = link(glmParams.link,ary.col_mean(colIds[colIds.length-1]));
      double ymu = ary.col_mean(colIds[colIds.length-1]);
      GLMTest tst = new GLMTest(this,s,ymu);
      tst.invoke(ary._key);
      return new GLM_Validation(this, tst._results[GLMTest.ERRORS]/tst._n, tst._results[GLMTest.H], tst._results[GLMTest.H0],tst._n);
    }
    public String toString(){
      StringBuilder res = new StringBuilder("GLM Model:\n");
      if(warnings != null){
        res.append("\tWarnings: " + Arrays.toString(warnings) + "\n");
      }
      res.append("\tCoefficients: " + Arrays.toString(beta) + "\n");
      res.append("\tNull Deviance: " + nullDeviance + "\n");
      res.append("\tResidual Deviance: " + residualDeviance + "\n");
      return res.toString();
    }
  }

  /**
   *
   * @author tomasnykodym
   *
   */
  public static class BinomialModel extends GLM_Model {
    double _threshold;
    double _case;

    public BinomialModel(BinomialArgs arg){
      _threshold= arg._threshold;
      _case = arg._case;
    }

    public GLM_Validation validateOn(ValueArray ary, Sampling s) {
      //double ymu = link(glmParams.link,ary.col_mean(colIds[colIds.length-1]));
      double ymu = ary.col_mean(colIds[colIds.length-1]);
      BinomialTest tst = new BinomialTest(this,s,ymu);
      tst.invoke(ary._key);
      return new BinomialValidation(this, tst._confMatrix, tst._n, tst._results[GLMTest.H0], tst._results[GLMTest.H]);
    }

  }
  public enum DataPreprocessing {
    NONE, NORMALIZE, STANDARDIZE, AUTO
  };

  public static final double BETA_EPS = 1e-8;


  public static final LSM_Params defaultLSMParams = new LSM_Params();


/**
 * Solve glm problem by iterative reqeighted least squqre method.
 * Repeatedly solves LSM problem with weights given by previous iteration until fixpoint is reached.
 *
 * @param ary
 * @param colIds
 * @param s
 * @param glmParams
 * @param lsmParams
 * @param fargs
 * @return
 */
  public static GLM_Model solve(ValueArray ary, int[] colIds, Sampling s,
      GLM_Params glmParams, LSM_Params lsmParams, FamilyArgs fargs) {
    int[] p = null;
    if(lsmParams == null)lsmParams = defaultLSMParams;
    // set the preprocessing flags
    switch( glmParams.preprocessing ) {
    case AUTO:
      if( lsmParams.n != Norm.NONE ) {
        p = new int[colIds.length];
        for( int i = 0; i < colIds.length - 1; ++i )
          if( (ary.col_mean(colIds[i]) != 0) || ary.col_sigma(colIds[i]) != 1 )
            p[i] = RowVecTask.STANDARDIZE_DATA;
      }
      break;
    case NORMALIZE:
      p = new int[colIds.length];
      for( int i = 0; i < colIds.length - 1; ++i )
        p[i] = RowVecTask.NORMALIZE_DATA;
      break;
    case STANDARDIZE:
      p = new int[colIds.length];
      for( int i = 0; i < colIds.length - 1; ++i )
        p[i] = RowVecTask.STANDARDIZE_DATA;
      break;
    case NONE:
      break;
    default:
      throw new Error("Invalid data preprocessing flag " + glmParams.preprocessing);
    }

    GLM_Model m = (glmParams.family == Family.binomial)? new BinomialModel((BinomialArgs)fargs):new GLM_Model();//colIds, beta, p, gp, lp)
    m.colIds = colIds;
    m.glmParams = glmParams;
    m.lsmParams = lsmParams;
    m.preprocessing = p;

    if(glmParams.family == Family.gaussian){
      LSMTask tsk = new LSMTask(colIds, s, colIds.length - 1,  lsmParams.constant, p);
      tsk.invoke(ary._key);
      m.n = tsk._n;
      m.beta = DLSM.solveLSM(tsk._xx, tsk._xy, lsmParams);
      return m;
    }
    double [] beta = new double [colIds.length];
    Arrays.fill(beta, glmParams.link.defaultBeta);
    double diff = 0;
    long N = 0;
    try{
      for(int i = 0; i != MAX_ITER; ++i) {
        //System.out.println("iteration: " + i + ", beta = " + Arrays.toString(beta));
        IRLSMTask tsk;
        switch(glmParams.family){
        case binomial:
          BinomialArgs bargs = (BinomialArgs)fargs;
          tsk = new BinomialTask(colIds, s, lsmParams.constant, beta, p, glmParams.link, ary.col_mean(colIds[colIds.length-1]),bargs);
          break;
        default:
          tsk = new IRLSMTask(colIds, s, lsmParams.constant, beta, p, glmParams.family, glmParams.link);
        }
        diff = 0;
        tsk.invoke(ary._key);

        N = tsk._n;
        tsk._beta = DLSM.solveLSM(tsk._xx, tsk._xy, m.lsmParams);
        if( beta != null ) for( int j = 0; j < beta.length; ++j )
          diff = Math.max(diff, Math.abs(beta[j] - tsk._beta[j]));
        else diff = Double.MAX_VALUE;
        beta = tsk._beta;
        if(diff < BETA_EPS)break;
      } 
    } catch (Exception e) {
      if(beta == null)throw new GLSMException("Failed to compute the data: " + e.getMessage());;
      m.warnings = new String[]{"Failed to converge due to NaNs"};
    }
    if(diff >= BETA_EPS)m.warnings = new String[]{"Failed to converge due to reaching max # iterations"};
    m.beta = beta;
    m.n = N;
    return m;
  }


/**
 * Compute crossvalidation. Randomly divide dataset into x-factor parts and then run xfactor times, each tim e excluding one part in the training and use it for testing.
 * Reports avg result of validation + first 20 individual models in case of binomial family.
 *
 * @param ary
 * @param colIds
 * @param xfactor
 * @param glmParams
 * @param lsmParams
 * @param fargs
 * @return
 */
  public static ArrayList<GLM_Validation> xValidate(ValueArray ary, int[] colIds, int xfactor, GLM_Params glmParams, LSM_Params lsmParams, FamilyArgs fargs) {
    ArrayList<GLM_Validation> individualModels = new ArrayList<GLM_Validation>();
    if( xfactor == 1 ) return individualModels;
    for( int x = 0; x < xfactor; ++x ) {
      Sampling s = new Sampling(x, xfactor, false);

      GLM_Model m = solve(ary, colIds, s, glmParams, lsmParams, fargs);
      GLM_Validation val = m.validateOn(ary, s.complement());
      if( !individualModels.isEmpty() ) individualModels.get(0).aggregate(val);
      else individualModels.add(val.clone());
      if( individualModels.size() <= 20 ) individualModels.add(val);
    }
    return individualModels;
  }

  /**
   * Task computing one round of logistic regression by iterative least square
   * method. Given beta_k, computes beta_(k+1). Works by transforming input
   * vector by link function and applying weights equal to inverse of variance
   * and  passing the transformed input to LSM.
   *
   * @author tomasnykodym
   *
   */
  public static class IRLSMTask extends LSMTask {
    double[] _beta;
    double   _origConstant;

    int      _f;
    int      _l;

    public IRLSMTask() {
    } // Empty constructor for the serializers

    public IRLSMTask(int[] colIds, Sampling s, int constant, double[] beta,
        int[] p, Family f, Link l) {
      super(colIds, s, colIds.length - 1, constant, p);
      _beta = beta;
      _f = f.ordinal();
      _l = l.ordinal();
    }

    @Override
    public void init(int xlen, int nrows) {
      super.init(xlen, nrows);
      _origConstant = _constant;
    }

    /**
     * Applies the link function on the input and calls
     * underlying LSM.
     *
     * Two steps are performed here:
     * 1) y is replaced by z, which is obtained by
     * Taylor expansion at the point of last estimate of y (x'*beta)
     * 2) Weight is applied to both x and y. Weight is the square root of inverse of variance of y at this
     * data point according to our model
     *
     */
    @Override
    public void map(double[] x) {
      Family f = Family.values()[_f];
      Link l = Link.values()[_l];
      double y = x[x.length-1];
      // transform input to the GLR according to Olga's slides
      // (glm lecture, page 12)
      // Step 1, compute the estimate of y according to previous model (old
      // beta)
      double gmu = 0.0;
      for( int i = 0; i < x.length - 1; ++i ) {
        gmu += x[i] * _beta[i];
      }
      // add the constant (constant/Intercept is not included in the x vector,
      // have to add it seprately)
      gmu += _origConstant * _beta[x.length - 1];
      // get the inverse to get esitamte of p(Y=1|X) according to previous model
      double mu = linkInv(l,gmu);
      double dgmu = linkDeriv(l,mu);
      x[x.length - 1] = gmu + (y - mu) * dgmu; // z = y approx by Taylor
                                               // expansion at the point of our
                                               // estimate (mu), done to avoid
                                               // log(0),log(1)
      // Step 2
      double vary = variance(f,mu); // variance of y accrodgin to our model

      // compute the weights (inverse of variance of z)
      double var = dgmu * dgmu * vary;
      // Apply the weight. We want each datapoint to have weight of inverse of
      // the variance of y at this point.
      // Since we compute x'x, we take sqrt(w) and apply it to both x and y
      // (we also compute X*y)
      double w = Math.sqrt(1 / var);
      for( int i = 0; i < x.length; ++i )
        x[i] *= w;
      _constant = _origConstant * w;
      super.map(x);
    }
  }

  /**
   * Specialization of IRLSM for binomial family. Values 0/1 are enforced.(_case = 1, everything else = 0)
   * @author tomasnykodym
   *
   */
  public static class BinomialTask extends IRLSMTask {
    double _case; // in
    long _caseCount; // out

    public BinomialTask(int [] colIds, Sampling s, int constant, int [] p, Link l, double ymu, BinomialArgs bargs){
      this(colIds, s, constant, null, p, l, ymu, bargs);
    }
    public BinomialTask(int [] colIds, Sampling s, int constant, double [] beta, int [] p, Link l, double ymu, BinomialArgs bargs){
      super(colIds,s,constant, beta, p,  Family.binomial,l);
      _case = bargs._case;
    }

    @Override
    public void map(double [] x){
      if(x[x.length-1] == _case){
        x[x.length-1] = 1.0;
        ++_caseCount;
      } else
        x[x.length-1] = 0.0;
      super.map(x);
    }
  }


  public static class GLMTest extends RowVecTask {
    // INPUTS
    public static final int CONSTANT = 0;
    public static final int BETA     = 1;
    // RESULTS
    public static final int ERRORS   = 0;
    public static final int H0       = 1;
    public static final int H        = 2;


    double[] _inputParams; // constant + beta
    double[] _results;

    double _ymu;
    int _familyOrd;
    int _linkOrd;
    transient Link _link;
    transient Family _family;

    public GLMTest() {
    }

    public GLMTest(GLM_Model m, Sampling s, double ymu) {
      super(m.colIds, s, true, m.preprocessing);
      _inputParams = new double[m.beta.length + BETA];
      _inputParams[CONSTANT] = m.lsmParams.constant;
      System.arraycopy(m.beta, 0, _inputParams, BETA, m.beta.length);
      _familyOrd = m.glmParams.family.ordinal();
      _linkOrd = m.glmParams.link.ordinal();
      _ymu = ymu;
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GLMTest other = (GLMTest) drt;
      _n += other._n;
      if( _results == null ) _results = other._results;
      else for( int i = 0; i < _results.length; ++i )
        _results[i] += other._results[i];
    }

    @Override
    protected void init(int xlen, int nrows) {
      _results = new double[3];
      _family = Family.values()[_familyOrd];
      _link = Link.values()[_linkOrd];
    }

    protected double getMu(double[] x) {
      double ym = 0;
      for( int i = 0; i < (x.length - 1); ++i )
        ym += x[i] * _inputParams[BETA + i];
      if( _inputParams[CONSTANT] != 0 )
        ym += _inputParams[CONSTANT] * _inputParams[BETA + x.length - 1];
      return ym;
    }

    protected double getYm(double[] x) {
      return linkInv(_link,getMu(x));
    }


    @Override
    void map(double[] x) {
      double mu = getYm(x);
      double y = x[x.length-1];
      double diff = y - linkInv(_link, mu);
      _results[ERRORS] += (diff) * (diff);
      _results[H] += deviance(_family, y, mu);
      _results[H0] += deviance(_family,y, _ymu);
    }

    @Override
    protected void cleanup() {
      _inputParams = null; // don't propagate beta back
    }

    public double err() {
      return (_results != null) ? _results[ERRORS] : 0;
    }
  }


  public static class BinomialTest extends GLMTest {
    double   _threshold;
    double _case;

    long[][] _confMatrix;

    public BinomialTest() {}

    public BinomialTest(BinomialModel m, Sampling s, double ymu) {
      super(m,s,ymu);
      _threshold = m._threshold;
      _case = m._case;
    }

    @Override
    protected void init(int xlen, int nrows) {
      _confMatrix = new long[2][2];
      super.init(xlen, nrows);
    }

    @Override
    void map(double[] x) {
      x[x.length - 1] = (x[x.length - 1] == _case)?1:0;
      super.map(x);
      double yr =x[x.length - 1];
      double p = getYm(x);
      int ym = (p > _threshold) ? 1 : 0;
      _confMatrix[ym][(int) yr] += 1;
    }

    @Override
    public void cleanup() {
      _results[ERRORS] = (_confMatrix[1][0] + _confMatrix[0][1]) / (double) _n;
    }

    @Override
    public void reduce(DRemoteTask drt) {
      super.reduce(drt);
      BinomialTest other = (BinomialTest) drt;
      if( _confMatrix == null ) _confMatrix = other._confMatrix;
      else for( int i = 0; i < _confMatrix.length; ++i )
        for( int j = 0; j < _confMatrix[i].length; ++j )
          _confMatrix[i][j] += other._confMatrix[i][j];
    }
  }

}
