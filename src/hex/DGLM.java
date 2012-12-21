package hex;

import hex.DLSM.DLSM_SingularMatrixException;
import hex.DLSM.LSMTask;
import hex.DLSM.LSM_Params;
import hex.Models.BinaryClassifierValidation;
import hex.RowVecTask.DataPreprocessing;
import hex.RowVecTask.Sampling;

import java.util.Arrays;

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
public class DGLM implements Models.ModelBuilder {

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
    double [] _wt = new double[]{1.0,1.0};

    public BinomialArgs(double t, double c, double [] wt){
      _threshold = t;
      _case = c;
      _wt = wt;
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


  public static final double BETA_EPS = 1e-8;
  public static final LSM_Params defaultLSMParams = new LSM_Params();

  GLM_Params _glmParams;
  LSM_Params _lsmParams;
  FamilyArgs _fargs;

/**
 * Solve glm problem by iterative reweighted least square method.
 * Repeatedly solves LSM problem with weights given by previous iteration until
 * a fixed point is reached.
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
    for( int i = 0; i < colIds.length; ++i ){
      colNames[i] = ary._cols[colIds[i]]._name;
      if( colNames[i] == null ) {
        colNames = null;
        break;
      }
    }
    //GLM_Model m = (glmParams.family == Family.binomial)? new BinomialModel((BinomialArgs)fargs):new GLM_Model();//colIds, beta, p, gp, lp)
    GLMModel m = (_glmParams.family == Family.binomial)?new GLMBinomialModel(colNames, colIds, pVals,null,_glmParams.link,_glmParams.family,0, (BinomialArgs)_fargs):new GLMModel(colNames,colIds, pVals, null, _glmParams.link, _glmParams.family, 0);
    if(_glmParams.family == Family.gaussian){
      LSMTask tsk = new LSMTask(colIds, s, colIds.length - 1,  _lsmParams.constant, pVals);
      tsk.invoke(ary._key);
      m._n = tsk._n;
      try {
        m._beta = DLSM.solveLSM(tsk._xx, tsk._xy, _lsmParams);
      }catch (DLSM_SingularMatrixException e){
        int n = 0;
        if(m._warnings != null){
          n = m._warnings.length;
          m._warnings = Arrays.copyOf(m._warnings, n + 1);
        } else
          m._warnings = new String[1];
        m._warnings[n] = "Failed to compute without normalization due to singular gram matrix. Rerun with L2 regularization and lambda = 1e-5";
        m._beta = e.res;
      }
      return m;
    }
    double [] beta = new double [colIds.length];
    Arrays.fill(beta, _glmParams.link.defaultBeta);
    double diff = 0;
    long N = 0;
    m._ymu = ary._cols[colIds[colIds.length-1]]._mean;
    try {
      for(int i = 0; i != MAX_ITER; ++i) {
        //System.out.println("iteration: " + i + ", beta = " + Arrays.toString(beta));
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
        try {
          m._beta = DLSM.solveLSM(tsk._xx, tsk._xy, _lsmParams);
        }catch (DLSM_SingularMatrixException e){
          int n = 0;
          if(m._warnings != null){
            n = m._warnings.length;
            m._warnings = Arrays.copyOf(m._warnings, n + 1);
          } else
            m._warnings = new String[1];
          m._warnings[n] = "Failed to compute without normalization due to singular gram matrix. Rerun with L2 regularization and lambda = 1e-5";
          m._beta = e.res;
        }

        tsk._beta = DLSM.solveLSM(tsk._xx, tsk._xy, _lsmParams);
        if( beta != null ) for( int j = 0; j < beta.length; ++j )
          diff = Math.max(diff, Math.abs(beta[j] - tsk._beta[j]));
        else diff = Double.MAX_VALUE;
        beta = tsk._beta;
        if(diff < BETA_EPS)break;
      }
    } catch (Exception e) {
      if(beta == null)throw new GLSMException("Failed to compute the data: " + e.getMessage());;
      m._warnings = new String[]{"Failed to converge due to NaNs"};
    }
    if(diff >= BETA_EPS)m._warnings = new String[]{"Failed to converge due to reaching max # iterations"};
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
    double   _w = 1.0;
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
      // have to add it separately)
      gmu += _origConstant * _beta[x.length - 1];
      // get the inverse to get estimate of p(Y=1|X) according to previous model
      double mu = linkInv(l,gmu);
      double dgmu = linkDeriv(l,mu);
      x[x.length - 1] = gmu + (y - mu) * dgmu; // z = y approx by Taylor
                                               // expansion at the point of our
                                               // estimate (mu), done to avoid
                                               // log(0),log(1)
      // Step 2
      double vary = variance(f,mu); // variance of y according to our model

      // compute the weights (inverse of variance of z)
      double var = dgmu * dgmu * vary;
      // Apply the weight. We want each data point to have weight of inverse of
      // the variance of y at this point.
      // Since we compute x'x, we take sqrt(w) and apply it to both x and y
      // (we also compute X*y)
      double w = Math.sqrt(1 / var)*_w;
      for( int i = 0; i < x.length; ++i )
        x[i] *= w;
      _constant = _origConstant * w;
      super.processRow(x);
    }
  }

  /**
   * Specialization of IRLSM for binomial family. Values 0/1 are enforced.(_case = 1, everything else = 0)
   */
  public static class BinomialTask extends IRLSMTask {
    double _case; // in
    long _caseCount; // out
    double [] _wt;

    public BinomialTask(int [] colIds, Sampling s, int constant, double [] beta, double[][] pVals, Link l,BinomialArgs bargs){
      super(colIds,s,constant, beta, pVals,  Family.binomial,l);
      _case = bargs._case;
      _wt = bargs._wt;
    }

    @Override
    public void processRow(double [] x){
      if(x[x.length-1] == _case){
        x[x.length-1] = 1.0;
        _w = _wt[1];
        ++_caseCount;
      } else {
        x[x.length-1] = 0.0;
        _w = _wt[0];
      }

      super.processRow(x);
    }
  }


  public static class GLMModel extends Models.NewModel {
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
  public static class GLMValidation extends Models.ModelValidation {
    double _nullDev;
    double _resDev;
    double _err;
    double _errVar;
    transient double _ymu;
    transient Link _l;
    transient Family _f;
    long _n;
    int _t = 1;

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

  }


  public static class GLMBinomialModel extends GLMModel {
    double _threshold = 0.5;
    double _case = 1.0;

    @Override
    public double getYr(double[] x) {
      return (x[x.length-1] == _case)?1.0:0.0;
    }

    public GLMBinomialModel(){}
    public GLMBinomialModel(String [] columNames, int [] colIds, double [][] pVals){
      super(columNames, colIds, pVals);
    }

    public GLMBinomialModel(String [] columnNames, int [] colIds, double[][] pVals, double [] b, Link l, Family f, double ymu, BinomialArgs args){
      super(columnNames, colIds, pVals,b,l,f,ymu);
      _case = args._case;
      _threshold = args._threshold;
    }



    @Override
    Models.ModelValidation makeValidation() {
      return new GLMBinomialValidation(_ymu,Link.values()[_link], Family.values()[_family],_threshold);
    }
  }

  public static class GLMBinomialValidation extends GLMValidation implements BinaryClassifierValidation {
    double _threshold;
    public long [][] _cm;
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
      GLMBinomialValidation v = (GLMBinomialValidation)other;
      _fpMean = (_t - 1.0) / _t * fp() + 1.0 / _t * v.fp();
      // recursive variance formula
      double newVar = (v.fp() - fp());
      _fpVar = ((_t - 1.0) / _t) * _fpVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _tpMean = (_t - 1.0) / _t * tp() + 1.0 / _t * tp();
      // recursive variance formula
      newVar = (v.tp() - tp());
      _tpVar = ((_t - 1.0) / _t) * _tpVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _tnMean = (_t - 1.0) / _t * tn() + 1.0 / _t * v.tn();
      // recursive variance formula
      newVar = (v.tn() - tn());
      _tnVar = ((_t - 1.0) / _t) * _tnVar + (1.0 / (_t - 1)) * newVar
          * newVar;

      _fnMean = (_t - 1.0) / _t * fn() + 1.0 / _t * v.fn();
      // recursive variance formula
      newVar = (v.fn() - fn());
      _fnVar = ((_t - 1.0) / _t) * _fnVar + (1.0 / (_t - 1)) * newVar
          * newVar;
      _aggregate = true;

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
      return _aggregate?_fnMean:(double)_cm[0][1]/(double)_n;
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
}
