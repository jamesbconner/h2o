package hex;

import hex.RowVecTask.Sampling;

import java.sql.RowIdLifetime;
import java.util.ArrayList;
import java.util.Arrays;

import water.*;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

/**
 * General Linear Regression (http://en.wikipedia.org/wiki/Linear_regression)
 * implementation based on least squared minimization.
 *
 * Solves problem of finding parameters bs s.t. sum((bi*xi + b0 - y)^2) = min
 * (in other words, y is modeled as y' = bi*x + b0 such that the sum of squared
 * errors (y - y')^2 is minized. Bs are computed by following equation:
 * (sum(x'*x)/n)^-1 * sum(x*y)/n, where: x is a (column) vector of input values
 * x' is transposed x y is response variable
 *
 * @author tomasnykodym
 *
 */
public class GLSM {

  public static class GLSMException extends RuntimeException {
    public GLSMException(String msg) {
      super(msg);
    }
  }

  public enum Family {
    gaussian, binomial
  }

  public enum Norm {
    NONE, // standard regression without any regularization
    L1, // LASSO
    L2; // ridge regression
  }

  Sampling _sampling;


  final static int L2_LAMBDA = 0;
  final static int L1_LAMBDA = 0;
  final static int L1_RHO    = 1;
  final static int L1_ALPHA  = 2;

  protected static double[] solveLSM(double[][] xxAry, double[] xyAry, Norm n, double [] nParams) {
    Matrix xx = new Matrix(xxAry.length, xxAry.length);
    // we only computed half of the symmetric matrix, now we need to fill the
    // rest before computing the inverse
    double d = 0.0;
    switch(n){
    case L1:
      d = nParams[L1_RHO];
      break;
    case L2:
      d = nParams[L2_LAMBDA];
      break;
    }
    for( int i = 0; i < xxAry.length; ++i ) {
      for( int j = 0; j < xxAry[i].length; ++j ) {
        if(i == j){
          xx.set(i, j, xxAry[i][j] + d);
        } else {
          xx.set(i, j, xxAry[i][j]);
          xx.set(j, i, xxAry[i][j]);
        }
      }
    }
    CholeskyDecomposition lu = new CholeskyDecomposition(xx);
    switch(n){
    case NONE:
    case L2:
      return lu.solve(new Matrix(xyAry, xyAry.length)).getColumnPackedCopy();
    case L1:
    {
      final int N = xyAry.length;
      final double ABSTOL = Math.sqrt(N) * 1e-4;
      final double RELTOL = 1e-2;
      double[] z = new double[N];
      double[] u = new double[N];
      Matrix xm= null;
      double kappa = nParams[L1_LAMBDA] / nParams[L1_RHO];
      for( int i = 0; i < 10000; ++i ) {
        // add rho*(z-u) to A'*y and add rho to diagonal of A'A
        Matrix xy = new Matrix(xyAry, N);
        for( int j = 0; j < N; ++j ) {
          xy.set(j, 0, xy.get(j, 0) + nParams[L1_RHO] * (z[j] - u[j]));
        }
        // updated x
        xm = lu.solve(xy);

        // vars to be used for stopping criteria
        double x_norm = 0;
        double z_norm = 0;
        double u_norm = 0;
        // update z and u
        double r_norm = 0;
        double s_norm = 0;
        double eps_pri = 0; // epsilon primal
        double eps_dual = 0;

        for( int j = 0; j < N; ++j ) {
          double x_hat = xm.get(j,0);
          x_norm += x_hat * x_hat;
          x_hat = x_hat * nParams[L1_ALPHA] + (1 - nParams[L1_ALPHA])*z[j];
          double zold = z[j];
          z[j] = shrinkage(x_hat + u[j], kappa);
          z_norm += z[j] * z[j];
          s_norm += (z[j] - zold) * (z[j] - zold);
          r_norm += (xm.get(j,0) - z[j]) * (xm.get(j,0) - z[j]);
          u[j] += x_hat - z[j];
          u_norm += u[j] * u[j];
        }
        r_norm = Math.sqrt(r_norm);
        s_norm = nParams[L1_RHO] * Math.sqrt(s_norm);
        eps_pri = ABSTOL + RELTOL * Math.sqrt(Math.max(x_norm, z_norm));
        eps_dual = ABSTOL + nParams[L1_RHO] * RELTOL * Math.sqrt(u_norm);
        if( r_norm < eps_pri && s_norm < eps_dual ) break;
      }
      return z;
    }
    default:
      throw new Error("Unexpected Norm " + n);
    }
  }

  // sampling vars
  int     _offset;
  int     _step;
  boolean _complement;

  public void setSampling(int offset, int step, boolean complement) {
    _offset = offset;
    _step = step;
    _complement = complement;
  }


  private static double shrinkage(double x, double kappa) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  public static class GLM_Validation implements Cloneable {
    GLM_Model _m;
    long      _n;
    double[]  _deviance; // deviance of h0 (Null) and h1
    double    _errMean;
    double    _errVar;
    int       _t;

    public GLM_Validation(GLM_Model m, double err, long n) {
      _m = m;
      _n = n;
      _errMean = err;
      _t = 1;
    }

    public GLM_Validation(GLM_Validation other) {
      _m = other._m;
      _n = other._n;
      _t = other._t;
      _errMean = other._errMean;
      _errVar = other._errVar;
      _t = other._t;
      if( other._deviance != null ) _deviance = other._deviance.clone();
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
      return (_deviance != null) ? -2 * _deviance[0] : 0.0;
    }

    public double residualDeviance() {
      return (_deviance != null) ? -2 * _deviance[1] : 0.0;
    }

    public GLM_Model m() {
      return _m;
    }

    @Override
    public GLM_Validation clone() {
      return new GLM_Validation(this);
    }
  }

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
      super(m, (sum(cm) - sum_diag(cm)) / (double) sum(cm), n);
      _deviance = new double[] { nullDeviance, residualDeviance };
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

  public static class GLM_Model {
    public final double[] beta;
    public final double   constant;
    int[]                 preprocessing;

    public final long     n;

    public GLM_Model(long n, double[] beta, int[] p) {
      this(n, beta, 1, p);
    }

    public GLM_Model(long n, double[] beta, double constant, int[] p) {
      this.n = n;
      this.beta = beta;
      this.constant = constant;
      preprocessing = p;
    }

    public double apply(double[] x) {
      double res = 0.0;
      for( int i = 0; i < x.length; ++i )
        res += beta[i] * x[i];
      if( constant != 0 ) res += constant * beta[x.length];
      return res;
    }

    public GLM_Validation validateOn(ValueArray ary, Sampling s, int[] colIds,
        double[] args) {
      LSMTest tst = new LSMTest(colIds, s, beta, constant, preprocessing);
      tst.invoke(ary._key);
      return new GLM_Validation(this, tst.err() / tst._n, tst._n);
    }

  }

  public static class BinomialModel extends GLM_Model {
    final double _b0;

    public BinomialModel(long n, double[] beta, double constant, double b0,
        int[] p) {
      super(n, beta, constant, p);
      _b0 = b0;
    }

    public double apply(double[] x) {
      return logitInv(super.apply(x));
    }

    public GLM_Validation validateOn(ValueArray ary, Sampling s, int[] colIds,
        double[] args) {
      BinomialTest tst = new BinomialTest(colIds, s, beta, _b0, constant,
          args[0], preprocessing);
      tst.invoke(ary._key);
      return new BinomialValidation(this, tst._confMatrix, tst._n,
          tst._results[BinomialTest.H0], tst._results[BinomialTest.H]);
    }
  }

  public enum DataPreprocessing {
    NONE, NORMALIZE, STANDARDIZE, AUTO
  };

  public static GLM_Model solve(ValueArray ary, int[] colIds, Sampling s,
      int c, Family f, Norm n, double[] nParams, DataPreprocessing preprocessing) {
    boolean ary_standardized = true;
    int[] p = null;
    switch( preprocessing ) {
    case AUTO:
      if( n != Norm.NONE ) {
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
      throw new Error("Invalid data preprocessing flag " + preprocessing);
    }

    switch( f ) {
    case gaussian: {
      LSMTask tsk = new LSMTask(colIds, s, colIds.length - 1, c, p);
      tsk.invoke(ary._key);
      return new GLM_Model(tsk._n, solveLSM(tsk._xx, tsk._xy, n, nParams), p);
    }
    case binomial: {
      // check we have only values 0,1 as y
      int y = colIds[colIds.length - 1];
      if( ary.col_max(y) != 1 || ary.col_min(y) != 0 )
        throw new GLSMException(
            "Logistic regression can only have values from range <0,1> as y column.");
      LogitLSMTask tsk = new LogitLSMTask(colIds, s, c, p);
      tsk.invoke(ary._key);
      double[] oldBeta = null;
      double[] beta = solveLSM(tsk._xx, tsk._xy, n, nParams);
      double[] beta_gradient = new double[beta.length];
      double diff = 0;
      do {
        if( oldBeta != null ) for( int i = 0; i < oldBeta.length; ++i )
          beta_gradient[i] = Math.abs(oldBeta[i] - beta[i]);
        oldBeta = beta;
        tsk = new LogitLSMTask(colIds, s, c, oldBeta, p);
        tsk.invoke(ary._key);
        beta = solveLSM(tsk._xx, tsk._xy, n, nParams);
        diff = 0;
        for( int i = 0; i < beta.length; ++i )
          diff += (oldBeta[i] - beta[i]) * (oldBeta[i] - beta[i]);
      } while( diff > 1e-5 );
      for( int i = 0; i < beta.length; ++i )
        if( Double.isNaN(beta[i]) ) {
          int maxJ = 0;
          for( int j = 1; j < beta_gradient.length - 1; ++j )
            if( beta_gradient[j] > beta_gradient[maxJ] ) maxJ = j;
          throw new GLSMException(
              "Obtained invalid beta. Try to use regularizationor or remove column "
                  + colIds[maxJ]);
        }
      return new BinomialModel(tsk._n, beta, c, tsk._ncases / (double) tsk._n,
          p);
    }
    default:
      throw new GLSMException("Unsupported family: " + f.toString());
    }
  }

  public static ArrayList<GLM_Validation> xValidate(ValueArray ary, Family f,
      int[] colIds, int xfactor, double threshold, int constant, Norm n,
      double[] args) {
    ArrayList<GLM_Validation> individualModels = new ArrayList<GLM_Validation>();
    if( xfactor == 1 ) return individualModels;
    for( int x = 0; x < xfactor; ++x ) {
      Sampling s = new Sampling(x, xfactor, false);
      GLM_Model m = solve(ary, colIds, s, 1, f, n, args, DataPreprocessing.AUTO);
      GLM_Validation val = m.validateOn(ary, s.complement(), colIds,
          new double[] { threshold });
      if( !individualModels.isEmpty() ) individualModels.get(0).aggregate(val);
      else individualModels.add(val.clone());
      if( individualModels.size() <= 20 ) individualModels.add(val);
    }
    return individualModels;
  }

  /**
   * Soft Thresholding operator as defined in ADMM paper, section 4.4.3
   *
   * @author tomasnykodym
   *
   */
  public static class S_Operator {
    double _k;

    public S_Operator() {
      this(0.0);
    }

    public S_Operator(double k) {
      assert k >= 0;
      _k = k;
    }

    public double call(double x) {
      if( x > _k ) return x - _k;
      if( x < -_k ) return x + _k;
      return 0.0;
    }
  }

  public static class LSMTask extends RowVecTask {
    double[][] _xx;      // matrix holding sum of x*x'
    double[]   _xy;      // vector holding sum of x * y
    int        _xfactor;
    double     _constant; // constant member
    double     _ymu;     // mean(y) estimate

    public LSMTask() {
    } // Empty constructors for the serializers

    public LSMTask(int[] colIds, int constant, int[] p) {
      this(colIds, null, colIds.length - 1, constant, p);
    }

    protected LSMTask(int[] colIds, Sampling s, int xlen, int constant, int[] p) {
      super(colIds, s, true, p);
      _constant = constant;
    }

    @Override
    public void init(int xlen, int nrows) {
      super.init(); // should always be called
      // the size is xlen (the columns read from the data object) + 1 for the
      // constant (Intercept)
      _xy = new double[xlen];
      _xx = new double[xlen][];
      for( int i = 0; i < xlen; ++i )
        _xx[i] = new double[i + 1];
    }

    /**
     * Body of the LR, computes sum(x'*x)/n and sum(x*y)/n for all rows in this
     * chunk. Since (x'*x) is symmetric, only the lower diagonal is computed.
     */
    @Override
    public void map(double[] x) {
      double y = x[x.length - 1];
      _ymu += y;
      // compute x*x' and add it to the marix
      for( int i = 0; i < (x.length - 1); ++i ) {
        for( int j = 0; j <= i; ++j ) { // matrix is symmetric, we only need to
                                        // compute 1/2
          _xx[i][j] += x[i] * x[j];
        }
        _xy[i] += x[i] * y;
      }
      // compute the constant (constant is not part of x and has to be computed
      // sepearetly)
      for( int j = 0; j < (x.length - 1); ++j )
        _xx[x.length - 1][j] += _constant * x[j];
      _xx[x.length - 1][x.length - 1] += _constant * _constant;
      _xy[x.length - 1] += _constant * y;
    }

    @Override
    public void cleanup() {
      // We divide by _n here, which is the number of rows processed in this
      // chunk, while
      // we really want to divide by N (the number of rows in the whole
      // dataset). The reason for this is
      // that there might be some missing values (and therefore omitted rows) so
      // we do not know N
      // at this point -> we divide by _n here and adjust for it later in
      // reduce.
      double nInv = 1.0 / _n;
      for( int i = 0; i < _xy.length; ++i ) {
        for( int j = 0; j <= i; ++j ) {
          _xx[i][j] *= nInv;
        }
        _xy[i] *= nInv;
      }
      _ymu *= nInv;
      super.cleanup();
    }

    /**
     * Add partial results.
     */
    @Override
    public void reduce(DRemoteTask drt) {
      LSMTask other = (LSMTask) drt;
      if( _xx != null || _xy != null ) {
        double R = 1.0 / (_n + other._n);
        double myR = other._n * R;
        double r = _n * R;
        _ymu = _ymu * myR + r * other._ymu;
        for( int i = 0; i < _xx.length; ++i ) {
          for( int j = 0; j <= i; ++j ) {
            _xx[i][j] = (myR * _xx[i][j] + r * other._xx[i][j]);
          }
          _xy[i] = (myR * _xy[i] + r * other._xy[i]);
        }
      } else {
        _xx = other._xx;
        _xy = other._xy;
      }
      _n += other._n;
    }
  }

  // derivative of logit
  static double logitPrime(double p) {
    if( p == 1 || p == 0 ) return 0;
    return 1 / (p * (1 - p));
  }

  // logit function
  static double logit(double x) {
    return Math.log(x) / Math.log(1 - x);
  }

  // inverse of logit
  static double logitInv(double x) {
    return 1.0 / (Math.exp(-x) + 1.0);
  }

  /**
   * Task computing one round of logistic regression by iterative least square
   * method. Given beta_k, computes beta_(k+1). Works by transforming input
   * vector by logit function and passing the transformed input to LSM.
   *
   * @author tomasnykodym
   *
   */
  public static class LogitLSMTask extends LSMTask {
    double[] _beta;
    double   _origConstant;
    long     _ncases;

    public LogitLSMTask() {
    } // Empty constructor for the serializers

    public LogitLSMTask(int[] colIds, Sampling s, int constant, double[] beta,
        int[] p) {
      super(colIds, s, colIds.length - 1, constant, p);
      _beta = beta;
    }

    public LogitLSMTask(int[] colIds, Sampling s, int constant, int[] p) {
      this(colIds, s, constant, new double[colIds.length
          - ((constant == 0) ? 1 : 0)], p);
    }

    @Override
    public void init(int xlen, int nrows) {
      super.init(xlen, nrows);
      _origConstant = _constant;
    }

    /**
     * Applies the link function (logit in this case) on the input and calls
     * underlying LSM.
     *
     * Two steps are performed here: 1) y is replaced by z, which is obtained by
     * Taylor expansion at the point of last estimate of y (x'*beta) 2) both x
     * and y are wighted by the square root of inverse of variance of y at this
     * data point according to our model
     *
     */
    @Override
    public void map(double[] x) {
      double y = x[x.length - 1];
      assert 0 <= y && y <= 1;
      _ncases += y;
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
      double mu = logitInv(gmu);
      double dgmu = logitPrime(mu);
      x[x.length - 1] = gmu + (y - mu) * dgmu; // z = y approx by Taylor
                                               // expansion at the point of our
                                               // estimate (mu), done to avoid
                                               // log(0),log(1)
      // Step 2
      double vary = mu * (1 - mu); // variance of y accrodgin to our model
                                   // (binomial distribution)
      // compute the weights (inverse of variance of z)
      double var = dgmu * dgmu * vary;
      // apply the weight, we want each datapoint to have weight of inverse of
      // the variance of y at this point.
      // since we compute x'x, we take sqrt(w) and apply it to both x and y
      // (since we also compute X*y)
      double w = Math.sqrt(1 / var);
      for( int i = 0; i < x.length; ++i )
        x[i] *= w;
      _constant = _origConstant * w;
      // step 3 performed by LSMTask.map
      super.map(x);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      _ncases += ((LogitLSMTask) drt)._ncases;
      super.reduce(drt);
    }
  }

  public static class LSMTest extends RowVecTask {
    // INPUTS
    public static final int CONSTANT = 0;
    public static final int BETA     = 1;
    // RESULTS
    public static final int ERRORS   = 0;
    public static final int H0       = 1;
    public static final int H        = 2;

    double[]                _inputParams; // [constant, b0] + beta
    double[]                _results;

    public LSMTest() {
    }

    public LSMTest(int[] colIds, Sampling s, double[] beta, double constant,
        int[] p) {
      super(colIds, s, true, p);
      _inputParams = new double[beta.length + BETA];
      _inputParams[CONSTANT] = constant;
      System.arraycopy(beta, 0, _inputParams, BETA, beta.length);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      LSMTest other = (LSMTest) drt;
      _n += other._n;
      if( _results == null ) _results = other._results;
      else for( int i = 0; i < _results.length; ++i )
        _results[i] += other._results[i];
    }

    @Override
    protected void init(int xlen, int nrows) {
      _results = new double[3];
    }

    protected double getYm(double[] x) {
      double ym = 0;
      for( int i = 0; i < (x.length - 1); ++i )
        ym += x[i] * _inputParams[BETA + i];
      if( _inputParams[CONSTANT] != 0 )
        ym += _inputParams[CONSTANT] * _inputParams[BETA + x.length - 1];
      return ym;
    }

    @Override
    void map(double[] x) {
      double diff = x[x.length - 1] - getYm(x);
      _results[ERRORS] += (diff) * (diff);
    }

    @Override
    protected void cleanup() {
      _inputParams = null; // don't propagate beta back
    }

    public double err() {
      return (_results != null) ? _results[ERRORS] : 0;
    }
  }

  /**
   * Lasso implementation, based on ADMM algorithm.
   *
   *
   * @author tomasnykodym
   *
   */
  public static class LASSO_Task extends LSMTask {

  }

  public static class BinomialTest extends LSMTest {
    double   _threshold;
    double   _b0;
    long[][] _confMatrix;

    public BinomialTest() {
    }

    public BinomialTest(int[] colIds, Sampling s, double[] beta, double b0,
        double constant, double threshold, int[] p) {
      super(colIds, s, beta, constant, p);
      _threshold = threshold;
      _b0 = b0;
    }

    @Override
    protected void init(int xlen, int nrows) {
      _confMatrix = new long[2][2];
      super.init(xlen, nrows);
    }

    @Override
    void map(double[] x) {
      double yr = x[x.length - 1];
      assert yr == 0 || yr == 1;
      double mu = getYm(x);
      double p = logitInv(mu);
      int ym = (p > _threshold) ? 1 : 0;
      _confMatrix[ym][(int) yr] += 1;
      if( _b0 > 0 ) {
        _results[H] += yr * mu - Math.log(1 + Math.exp(mu));
        _results[H0] += (yr == 1) ? Math.log(_b0) : Math.log(1 - _b0);
      }
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
      _results[ERRORS] = (_confMatrix[1][0] + _confMatrix[0][1]) / (double) _n;
    }
  }

}
