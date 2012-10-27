package hex;

import hex.DLSM.LSMTask;
import hex.DLSM.LSM_Params;
import hex.Models.BinaryClassifierValidation;
import hex.RowVecTask.DataPreprocessing;
import hex.RowVecTask.Sampling;

import java.util.Arrays;

import water.*;

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
public class DGLM implements Models.ModelBuilder {

  static final double DEFAULT_EPS = 1e-8;

  public static enum Link {
    identity(0),
    logit(0),
    log(0),
//    probit(0),
//    cauchit(0),
//    cloglog(0),
//    sqrt(0),
    inverse(1.0),
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
    gamma(Link.inverse);
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

  public DGLM(GLM_Params glmPArams, LSM_Params lsmParams, FamilyArgs fargs) {
    _glmParams = glmPArams;
    _lsmParams = lsmParams;
    _fargs = fargs;
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
    case gamma:
      return mu*mu;
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
    case gamma:
      if(yr == 0)return -2;
      return -2*(Math.log(yr/ym) - (yr - ym)/ym);
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
      case inverse:
        return 1/x;
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
      case inverse:
        return -1/(x*x);
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
    double[][]   pVals;
    public String[]       warnings;
    public long     n;
    public GLM_Model(){}
    public GLM_Model(long n, double[] beta, double [][] pVals, GLM_Params gp, LSM_Params lp) {
      this(n, beta, pVals, gp, lp, null);
    }
    public GLM_Model(long n, double[] beta, double[][] pVals,GLM_Params gp, LSM_Params lp,
        String[] warnings) {
      this.n = n;
      this.beta = beta;
      this.warnings = warnings;
      this.pVals = pVals;
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


  public static final double BETA_EPS = 1e-8;


  public static final LSM_Params defaultLSMParams = new LSM_Params();

  GLM_Params _glmParams;
  LSM_Params _lsmParams;
  FamilyArgs _fargs;

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
  public GLMModel trainOn(ValueArray ary, int[] colIds, Sampling s) {
    if(_lsmParams == null)_lsmParams = defaultLSMParams;
    DataPreprocessing dp = _glmParams.preprocessing;
    if(_glmParams.preprocessing == DataPreprocessing.AUTO){
      // default is to standardize data if using penalty function and do nothing if not
      if(_lsmParams.n != Norm.NONE) dp = DataPreprocessing.STANDARDIZE;
      else dp = DataPreprocessing.NONE;
    }
    double[][] pVals = RowVecTask.getDataPreprocessingForColumns(dp, ary, colIds);
    if(pVals != null){
      // do not preprocess y!!!
      pVals[pVals.length-1][0] = 0;
      pVals[pVals.length-1][1] = 0;
    }
    String [] colNames = new String [colIds.length];
    for(int i = 0; i < colIds.length; ++i){
      colNames[i] = ary.col_name(colIds[i]);
      if(colNames[i] == null){
        colNames = null;
        break;
      }
    }
    //GLM_Model m = (glmParams.family == Family.binomial)? new BinomialModel((BinomialArgs)fargs):new GLM_Model();//colIds, beta, p, gp, lp)
    GLMModel m = (_glmParams.family == Family.binomial)?new GLMBinomialModel(colNames, colIds, pVals,null,_glmParams.link,_glmParams.family,0):new GLMModel(colNames,colIds, pVals, null, _glmParams.link, _glmParams.family, 0);

    if(_glmParams.family == Family.gaussian){
      LSMTask tsk = new LSMTask(colIds, s, colIds.length - 1,  _lsmParams.constant, pVals);
      tsk.invoke(ary._key);
      m._n = tsk._n;
      m._beta = DLSM.solveLSM(tsk._xx, tsk._xy, _lsmParams);
      return m;
    }
    double [] beta = new double [colIds.length];
    Arrays.fill(beta, _glmParams.link.defaultBeta);
    double diff;
    long N = 0;
    m._ymu = ary.col_mean(colIds[colIds.length-1]);
    try{
      do {
        IRLSMTask tsk;
        switch(_glmParams.family){
        case binomial:
          BinomialArgs bargs = (BinomialArgs)_fargs;
          tsk = new BinomialTask(colIds, s, _lsmParams.constant, beta, pVals, _glmParams.link,bargs);
          tsk.invoke(ary._key);
          m._ymu = ((BinomialTask)tsk)._caseCount/(double)tsk._n;
          break;
        default:
          tsk = new IRLSMTask(colIds, s, _lsmParams.constant, beta, pVals, _glmParams.family, _glmParams.link);
          tsk.invoke(ary._key);

        }
        diff = 0;

        N = tsk._n;
        tsk._beta = DLSM.solveLSM(tsk._xx, tsk._xy, _lsmParams);
        if( beta != null ) for( int i = 0; i < beta.length; ++i )
          diff = Math.max(diff, Math.abs(beta[i] - tsk._beta[i]));
        else diff = Double.MAX_VALUE;
        beta = tsk._beta;
      } while( diff > BETA_EPS );
    } catch (Exception e) {
      if(beta == null)throw new GLSMException("Failed to compute the data: " + e.getMessage());;
      m._warnings = new String[]{"Failed to converge"};
    }
    if(_glmParams.family == Family.binomial){

    }
    m._beta = beta;
    m._n = N;
    return m;
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
        double[][] pVals, Family f, Link l) {
      super(colIds, s, colIds.length - 1, constant, pVals);
      _beta = beta;
      _f = f.ordinal();
      _l = l.ordinal();
    }

    @Override
    public void preMap(int xlen, int nrows) {
      super.preMap(xlen, nrows);
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
    public void processRow(double[] x) {
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
      super.processRow(x);
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


    public BinomialTask(int [] colIds, Sampling s, int constant, double [] beta, double[][] pVals, Link l,BinomialArgs bargs){
      super(colIds,s,constant, beta, pVals,  Family.binomial,l);
      _case = bargs._case;
    }

    @Override
    public void processRow(double [] x){
      if(x[x.length-1] == _case){
        x[x.length-1] = 1.0;
        ++_caseCount;
      } else
        x[x.length-1] = 0.0;
      super.processRow(x);
    }
  }


  public static class GLMModel extends Models.Model {
    double [] _beta;
    int [] _colIds;
    int _link;
    int _family;

    public GLMModel(){}

    public GLMModel(String [] columnNames, int [] colIds, double [][] pVals) {
      this(columnNames, colIds, pVals, null, Link.identity, Family.gaussian, 0.0);
    }

    public GLMModel(String [] columnNames,int [] colIds, double[][] pVals, double [] b, Link l, Family f, double ymu) {
      super(columnNames,colIds, pVals);
      _beta = b;
      _link = l.ordinal();
      _family = f.ordinal();
      _ymu = ymu;
    }

    public double [] beta(){return _beta;}
    @Override
    public boolean skipIncompleteLines() {
      return true;
    }

    public double getMu(double[] x) {
      double ym = 0;
      for( int i = 0; i < (_beta.length-1); ++i )
        ym += x[i] * _beta[i];
      ym += _beta[_beta.length - 1];
      return ym;
    }

    public double getYm(double[] x) {
      return linkInv(Link.values()[_link],getMu(x));
    }

    @Override
    Models.ModelValidation makeValidation() {
      return new GLMValidation(_ymu, Link.values()[_link], Family.values()[_family]);
    }
  }
  public static class GLMValidation extends RemoteTask implements Models.ModelValidation {
    double _nullDev;
    double _resDev;
    double _err;
    double _errVar;
    transient double _ymu;
    transient Link _l;
    transient Family _f;
    long _n;
    int _t;

    public GLMValidation(double ymu, Link l, Family f){
      _ymu = ymu;
      _l = l;
      _f = f;
    }

    public GLMValidation(GLMValidation other){
      _nullDev = other._nullDev;
      _resDev = other._resDev;
      _err = other._err;
      _errVar = other._errVar;
      _ymu = other._ymu;
      _l = other._l;
      _f = other._f;
      _n = other._n;
      _t = other._t;
    }

    @Override
    public void add(double yr, double ym) {
      _nullDev += deviance(_f, yr, _ymu);
      _resDev += deviance(_f, yr, ym);
      _err += (yr-ym)*(yr-ym);
      ++_n;
    }

    @Override
    public void add(Models.ModelValidation other) {
      GLMValidation v = (GLMValidation)other;
      _nullDev += v._nullDev;
      _resDev += v._resDev;
      _n += v._n;
      _err += v._err;
    }

    @Override
    public void aggregate(Models.ModelValidation mv) {
      GLMValidation other = (GLMValidation)mv;
      // recursive avg formula
      _n += other._n;
      ++_t;
      _err = (_t - 1.0) / _t * _err + 1.0 / _t * other._err;
      // recursive variance formula
      double newVar = (other._err - _err);
      _errVar = ((_t - 1.0) / _t) * _errVar + (1.0 / (_t - 1)) * newVar
          * newVar;
    }

    @Override
    public double err() {
      return _err;
    }

    public double nullDeviance(){return _nullDev;}
    public double resDeviance(){return _resDev;}

    @Override
    public Models.ModelValidation clone() {
      return new GLMValidation(this);
    }

    @Override
    public long n() {
      return _n;
    }

    @Override
    public void invoke(Key args) {
      throw new RuntimeException("TODO Auto-generated method stub");
    }

    @Override
    public void compute() {
      throw new RuntimeException("TODO Auto-generated method stub");
    }


  }

  public static class GLMBinomialModel extends GLMModel {
    double _threshold = 0.5;



    public GLMBinomialModel(){}
    public GLMBinomialModel(String [] columNames, int [] colIds, double [][] pVals){
      super(columNames, colIds, pVals);
    }
    public GLMBinomialModel(String [] columnNames, int [] colIds, double[][] pVals, double [] b, Link l, Family f, double ymu){
      super(columnNames, colIds, pVals,b,l,f,ymu);
    }



    @Override
    Models.ModelValidation makeValidation() {
      return new GLMBinomialValidation(_ymu,Link.values()[_link], Family.values()[_family],_threshold);
    }
  }

  public static class GLMBinomialValidation extends GLMValidation implements BinaryClassifierValidation {
    double _threshold;
    long [][] _cm;
    double _fpMean;
    double _fpVar;
    double _fnMean;
    double _fnVar;
    double _tnMean;
    double _tnVar;
    double _tpMean;
    double _tpVar;
    boolean _aggregate;

    public GLMBinomialValidation(double ymu, Link l, Family f, double threshold){
      super(ymu,l,f);
      _threshold = threshold;
      _cm = new long[2][2];
    }

    public GLMBinomialValidation(GLMBinomialValidation other){
      super(other);
      _threshold = other._threshold;
      _cm = other._cm.clone();
    }



    @Override
    public void add(double yr, double ym) {
      assert !_aggregate;
      super.add(yr,ym);
      int m = (ym > _threshold)?1:0;
      int r = (int)yr;
      assert r == yr;
      ++_cm[m][r];
    }

    @Override
    public void aggregate(Models.ModelValidation other) {
      super.aggregate(other);
      _aggregate = true;
      GLMBinomialValidation v = (GLMBinomialValidation)other;
      _fpMean = (_t - 1.0) / _t * _fpMean + 1.0 / _t * v._fpMean;
      // recursive variance formula
      double newVar = (v._fpMean - _fpMean);
      _fpVar = ((_t - 1.0) / _t) * _fpVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _tpMean = (_t - 1.0) / _t * _tpMean + 1.0 / _t * v._tpMean;
      // recursive variance formula
      newVar = (v._tpMean - _tpMean);
      _tpVar = ((_t - 1.0) / _t) * _tpVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _tnMean = (_t - 1.0) / _t * _tnMean + 1.0 / _t * v._tnMean;
      // recursive variance formula
      newVar = (v._tnMean - _tnMean);
      _tnVar = ((_t - 1.0) / _t) * _tnVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _fnMean = (_t - 1.0) / _t * _fnMean + 1.0 / _t * v._fnMean;
      // recursive variance formula
      newVar = (v._fnMean - _fnMean);
      _fnVar = ((_t - 1.0) / _t) * _fnVar + (1.0 / (_t - 1)) * newVar
          * newVar;

    }

    public long cm(int i, int j){
      return _cm[i][j];
    }

    @Override
    public void add(Models.ModelValidation other) {
      super.add(other);
      GLMBinomialValidation bv = (GLMBinomialValidation)other;
      for(int i = 0; i < _cm.length; ++i)
        for(int j = 0; j < _cm.length; ++j)
          _cm[i][j] += bv._cm[i][j];
      _err = (_cm[0][1] + _cm[1][0])/(double)_n;
    }

    @Override
    public double err() {
      if(_n == 0)return 0;
      return  (_cm[0][1] + _cm[1][0])/(double)_n;
    }

    public double fp(){
      return (_aggregate?_fpMean:(double)_cm[1][0]/(double)_n);
    }
    public double fpVar(){
      return _fpVar;
    }

    public double fn(){
      return _aggregate?_tnMean:(double)_cm[0][1]/(double)_n;
    }

    public double fnVar(){
      return _fnVar;
    }

    public double tp(){
      return _aggregate?_tpMean:(double)_cm[1][1]/(double)_n;
    }

    public double tpVar(){
      return _tpVar;
    }

    public double tn(){
      return _aggregate?_tnMean:(double)_cm[0][0]/(double)_n;
    }
    public double tnVar(){
      return _tnVar;
    }

    @Override
    public int classes() {
      return 2;
    }

    @Override
    public GLMBinomialValidation clone() {
      return new GLMBinomialValidation(this);
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
      super(m.colIds, s, true, m.pVals);
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
    protected void preMap(int xlen, int nrows) {
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
    void processRow(double[] x) {
      double mu = getYm(x);
      double y = x[x.length-1];
      double diff = y - linkInv(_link, mu);
      _results[ERRORS] += (diff) * (diff);
      _results[H] += deviance(_family, y, mu);
      _results[H0] += deviance(_family,y, _ymu);
    }

    @Override
    protected void postMap() {
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
    protected void preMap(int xlen, int nrows) {
      _confMatrix = new long[2][2];
      super.preMap(xlen, nrows);
    }

    @Override
    void processRow(double[] x) {
      x[x.length - 1] = (x[x.length - 1] == _case)?1:0;
      super.processRow(x);
      double yr =x[x.length - 1];
      double p = getYm(x);
      int ym = (p > _threshold) ? 1 : 0;
      _confMatrix[ym][(int) yr] += 1;
    }

    @Override
    public void postMap() {
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
