package hex;
import Jama.Matrix;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import water.*;

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

  ValueArray _ary;
  int[]    _colIds;
  int      _c;
  Family   _f;
  LSMTask  _tsk;
  double[] _beta;

  public GLSM(ValueArray ary, int[] colIds, int c, Family f) {
    _ary = ary;
    _colIds = colIds;
    _c = c;
    _f = f;
  }

  public static class GLSMException extends RuntimeException {
    public GLSMException(String msg) {
      super(msg);
    }
  }

  public enum Family {
    gaussian, binomial
  }

  public long n() {
    return (_tsk != null) ? _tsk._n : 0;
  }

  protected double[] solve2() {
    _tsk.invoke(_ary._key);
    Matrix xx = new Matrix(_tsk._xx.length, _tsk._xx.length);
    // we only computed half of the symmetric matrix, now we need to fill the
    // rest before computing the inverse
    for( int i = 0; i < _tsk._xx.length; ++i ) {
      for( int j = 0; j < _tsk._xx[i].length; ++j ) {
        xx.set(i, j, _tsk._xx[i][j]);
        xx.set(j, i, _tsk._xx[i][j]);
      }
    }
    try {
      xx = xx.inverse();
    } catch( RuntimeException e ) {
      throw new GLSMException("can not perform LSM on this data, obtained matrix is singular!");
    }
    return xx.times(new Matrix(_tsk._xy, _tsk._xy.length)).getColumnPackedCopy();
  }

  public double[] solve() {
    switch( _f ) {
    case gaussian:
      _tsk = new LSMTask(_colIds, _colIds.length - 1, _c);
      return solve2();
    case binomial: {
      // check we have only values 0,1 as y
      int y = _colIds[_colIds.length-1];
      if(_ary.col_max(y) != 1 || _ary.col_min(y) != 0)
        throw new GLSMException("Logistic regression can only have values from range <0,1> as y column.");
      _tsk = new LogitLSMTask(_colIds, _c);
      double[] oldBeta;
      _beta = solve2();
      double diff = 0;
      do {
        oldBeta = _beta;
        _tsk = new LogitLSMTask(_colIds, _c, oldBeta);
        _beta = solve2();
        diff = 0;
        for( int i = 0; i < _beta.length; ++i )
          diff += (oldBeta[i] - _beta[i]) * (oldBeta[i] - _beta[i]);
      } while( diff > 1e-5 );
      // now validate the input
      return _beta;
    }
    default:
      throw new GLSMException("Unsupported family: " + _f.toString());
    }
  }

  public double[][] xValidate(int xfactor, double threshold) {
    double [][] confusionMatrix = {{0,0},{0,0}};
    for( int i = 0; i < xfactor; ++i ) {
      long seed = System.currentTimeMillis();
      switch( _f ) {
      case gaussian:
        throw new GLSMException("Cross validation not supported for gaussian family");
      case binomial: {
        if(0 > threshold || threshold > 1)throw new GLSMException("illegal decision threshold! number between 0 and 1 expected, got " + threshold);
        _tsk = new LogitLSMTask(_colIds, _c);
        _tsk.setSampling(seed, xfactor, false);
        double[] oldBeta;
        _beta = solve2();
        double diff = 0;
        do {
          oldBeta = _beta;
          _tsk = new LogitLSMTask(_colIds, _c, oldBeta);
          _beta = solve2();
          diff = 0;
          for( int j = 0; j < _beta.length; ++j )
            diff += (oldBeta[j] - _beta[j]) * (oldBeta[j] - _beta[j]);
        } while( diff > 1e-5 );
        // now validate the input
        BinomialXValidateTask xTask= new BinomialXValidateTask(_colIds, _beta, threshold);
        xTask.setSampling(seed, xfactor, true);
        xTask.invoke(_ary._key);
        try {xTask.get();} catch( Exception e ) {throw new RuntimeException(e);}
        confusionMatrix[0][0] += xTask._confMatrix[0][0];
        confusionMatrix[0][1] += xTask._confMatrix[0][1];
        confusionMatrix[1][0] += xTask._confMatrix[1][0];
        confusionMatrix[1][1] += xTask._confMatrix[1][1];
      }
        break;
      default:
        throw new GLSMException("Unsupported family: " + _f.toString());
      }
    }
    double d = 1.0/(_tsk._n);
    confusionMatrix[0][0] *= d;
    confusionMatrix[0][1] *= d;
    confusionMatrix[1][0] *= d;
    confusionMatrix[1][1] *= d;
    return confusionMatrix;
  }

  public double[] test() {
    switch( _f ) {
    case gaussian:
      return null; // unimplemented for now
    case binomial: {
      BinomialTest tst = new BinomialTest(_colIds, _beta,
          ((LogitLSMTask) _tsk)._ncases / (double) _tsk._n, _c);
      tst.invoke(_ary._key);
      try {
        tst.get();
      } catch( Exception e ) {
        throw new RuntimeException(e);
      }
      return tst._results;
    }
    default:
      throw new Error("unexpected family " + _f);
    }

  }

  public static class LSMTask extends RowVecTask {
    double[][] _xx;      // matrix holding sum of x*x'
    double[]   _xy;      // vector holding sum of x * y
    double     _constant; // constant member
    long       _n;       // number of valid rows in this chunk
    double     _ymu;     // mean(y) estimate

    public LSMTask(int[] colIds, int constant) {
      this(colIds, colIds.length - 1, constant);
    }

    protected LSMTask(int[] colIds, int xlen, int constant) {
      super(colIds);
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
      for( double d : x )
        if( Double.isNaN(d) ) return; // skip incomplete rows
      ++_n;
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

    public LogitLSMTask(int[] colIds, int constant, double[] beta) {
      super(colIds, colIds.length - 1, constant);
      _beta = beta;
    }

    public LogitLSMTask(int[] colIds, int constant) {
      this(colIds, constant, new double[colIds.length
          - ((constant == 0) ? 1 : 0)]);
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
      for( double v : x )
        if( Double.isNaN(v) ) return;
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

  public static class BinomialXValidateTask extends RowVecTask {
    double [] _beta;
    double _threshold;
    int [][] _confMatrix = {{0,0},{0,0}};

    public BinomialXValidateTask() {
    }

    public BinomialXValidateTask(int[] colIds, double[] beta, double threshold) {
      super(colIds);
      _beta = beta;
      _threshold = threshold;
    }

    @Override
    void map(double[] x) {
      for( double v : x )
        if( Double.isNaN(v) ) return;
      double mu = _beta[_beta.length-1];
      int yr = (int)x[x.length - 1];
      assert yr == 0 || yr == 1;
      for( int i = 0; i < (x.length - 1); ++i )
        mu += x[i] * _beta[i];
      double p = logitInv(mu);
      int ym = (p > _threshold)?1:0;
      _confMatrix[ym][yr] += 1;
    }
    @Override
    public void reduce(DRemoteTask drt) {
      BinomialXValidateTask other = (BinomialXValidateTask)drt;
      if(_confMatrix == null)
        _confMatrix = other._confMatrix;
      else {
        _confMatrix[0][0] += other._confMatrix[0][0];
        _confMatrix[0][1] += other._confMatrix[0][1];
        _confMatrix[1][0] += other._confMatrix[1][0];
        _confMatrix[1][1] += other._confMatrix[1][1];
      }
    }
  }

  public static class BinomialTest extends RowVecTask {
    // INPUTS
    static final int CONSTANT = 0;
    static final int B0       = 1;
    static final int BETA     = 2;
    // RESULTS
    static final int H0       = 0;
    static final int H        = 1;

    double[]         _inputParams; // [constant, b0] + beta
    double[]         _results;

    public BinomialTest() {
    }

    public BinomialTest(int[] colIds, double[] beta, double b0, double constant) {
      super(colIds);
      _inputParams = new double[beta.length + BETA];
      _inputParams[B0] = b0;
      _inputParams[CONSTANT] = constant;
      System.arraycopy(beta, 0, _inputParams, BETA, beta.length);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      BinomialTest other = (BinomialTest) drt;
      if( _results == null ) _results = other._results;
      else for( int i = 0; i < _results.length; ++i )
        _results[i] += other._results[i];
    }

    @Override
    protected void init(int xlen, int nrows) {
      _results = new double[2];
    }

    @Override
    void map(double[] x) {
      for( double v : x )
        if( Double.isNaN(v) ) return;
      double mu = 0;
      double yr = x[x.length - 1];
      assert yr == 0 || yr == 1;
      for( int i = 0; i < (x.length - 1); ++i )
        mu += x[i] * _inputParams[BETA + i];
      if( _inputParams[CONSTANT] != 0 )
        mu += _inputParams[CONSTANT] * _inputParams[BETA + x.length - 1];
      _results[H] += yr * mu - Math.log(1 + Math.exp(mu));
      _results[H0] += (yr == 1) ? Math.log(_inputParams[B0]) : Math
          .log(1 - _inputParams[B0]);

    }

    @Override
    protected void cleanup() {
      _inputParams = null; // don't propagate beta back
    }
  }

}
