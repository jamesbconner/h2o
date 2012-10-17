package hex;

import hex.RowVecTask.Sampling;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.mapred.machines_jsp;

import water.DRemoteTask;
import water.ValueArray;
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

  static final double DEFAULT_EPS = 1e-8;

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

  public static enum Family {
    gaussian(Link.identity),
    binomial(Link.logit),
    poisson(Link.log),
    //gamma(Link.inverse);
    ;
    public final Link defaultLink;

    Family(Link l){defaultLink = l;}
  }

  public static enum Norm {
    NONE, // standard regression without any regularization
    L1,   // LASSO
    L2;   // ridge regression
  }

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


  static double y_log_y(double y, double mu){
    mu = Math.max(Double.MIN_NORMAL, mu);
    return (y != 0) ? (y * Math.log(y/mu)) : 0;
  }
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


  public static abstract class FamilyArgs{}

  public static class BinomialArgs extends FamilyArgs {
    double _threshold = 0.5;
    double _case = 1.0;

    public BinomialArgs(double t, double c){
      _threshold = t;
      _case = c;
    }
  }


  public static class LSM_Params{
    public Norm n = Norm.NONE;
    public double lambda = 0;
    public double rho = 0;
    public double alpha = 0;
    int constant = 1;

    public LSM_Params(){}
    public LSM_Params(Norm n, double lambda, double rho, double alpha, int constant){
      this.n = n;
      this.lambda = lambda;
      this.rho = rho;
      this.alpha = alpha;
      this.constant = constant;
    }
  }

  public static class GLM_Params{
    double beta_eps = GLSM.DEFAULT_EPS;
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

  final static int L2_LAMBDA = 0;
  final static int L1_LAMBDA = 0;
  final static int L1_RHO    = 1;
  final static int L1_ALPHA  = 2;

  protected static double[] solveLSM(double[][] xxAry, double[] xyAry,
      LSM_Params params) {
    Matrix xx = new Matrix(xxAry.length, xxAry.length);
    // we only computed half of the symmetric matrix, now we need to fill the
    // rest before computing the inverse
    double d = 0.0;
    switch( params.n ) {
    case NONE:
      break;
    case L1:
      d = params.rho;
      break;
    case L2:
      d = params.lambda;
      break;
    }
    for( int i = 0; i < xxAry.length; ++i ) {
      for( int j = 0; j < xxAry[i].length; ++j ) {
        if( i == j ) {
          xx.set(i, j, xxAry[i][j] + d);
        } else {
          xx.set(i, j, xxAry[i][j]);
          xx.set(j, i, xxAry[i][j]);
        }
      }
    }
    CholeskyDecomposition lu = new CholeskyDecomposition(xx);

    switch( params.n ) {
    case NONE:
    case L2:
      // return xx.inverse().times(new Matrix(xyAry,
      // xyAry.length)).getColumnPackedCopy();
      return lu.solve(new Matrix(xyAry, xyAry.length)).getColumnPackedCopy();
    case L1: {
      final int N = xyAry.length;
      final double ABSTOL = Math.sqrt(N) * 1e-4;
      final double RELTOL = 1e-2;
      double[] z = new double[N];
      double[] u = new double[N];
      Matrix xm = null;
      double kappa = params.lambda / params.rho;
      for( int i = 0; i < 10000; ++i ) {
        // add rho*(z-u) to A'*y and add rho to diagonal of A'A
        Matrix xy = new Matrix(xyAry, N);
        for( int j = 0; j < N; ++j ) {
          xy.set(j, 0, xy.get(j, 0) + params.rho * (z[j] - u[j]));
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
          double x_hat = xm.get(j, 0);
          x_norm += x_hat * x_hat;
          x_hat = x_hat * params.alpha + (1 - params.alpha) * z[j];
          double zold = z[j];
          z[j] = shrinkage(x_hat + u[j], kappa);
          z_norm += z[j] * z[j];
          s_norm += (z[j] - zold) * (z[j] - zold);
          r_norm += (xm.get(j, 0) - z[j]) * (xm.get(j, 0) - z[j]);
          u[j] += x_hat - z[j];
          u_norm += u[j] * u[j];
        }
        r_norm = Math.sqrt(r_norm);
        s_norm = params.rho * Math.sqrt(s_norm);
        eps_pri = ABSTOL + RELTOL * Math.sqrt(Math.max(x_norm, z_norm));
        eps_dual = ABSTOL + params.rho * RELTOL * Math.sqrt(u_norm);
        if( r_norm < eps_pri && s_norm < eps_dual ) break;
      }
      return z;
    }
    default:
      throw new Error("Unexpected Norm " + params.n);
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
      LSMTest tst = new LSMTest(this,s,ymu);
      tst.invoke(ary._key);
      return new GLM_Validation(this, tst._results[LSMTest.ERRORS]/tst._n, tst._results[LSMTest.H], tst._results[LSMTest.H0],tst._n);
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
      return new BinomialValidation(this, tst._confMatrix, tst._n, tst._results[LSMTest.H0], tst._results[LSMTest.H]);
    }

  }
  public enum DataPreprocessing {
    NONE, NORMALIZE, STANDARDIZE, AUTO
  };

  public static final double BETA_EPS = 1e-8;


  public static final LSM_Params defaultLSMParams = new LSM_Params();

  static int [] getDataPreprocessing(ValueArray ary, int [] colIds, DataPreprocessing p, Norm n){
    int [] res = null;
    switch(p){
    case AUTO:
      if(n != Norm.NONE ) {
        res = new int[colIds.length];
        for( int i = 0; i < colIds.length - 1; ++i )
          if( (ary.col_mean(colIds[i]) != 0) || ary.col_sigma(colIds[i]) != 1 )
            res[i] = RowVecTask.STANDARDIZE_DATA;
      }
      break;
    case NORMALIZE:
      res = new int[colIds.length];
      for( int i = 0; i < colIds.length - 1; ++i )
        if( (ary.col_min(colIds[i]) != 0) || ary.col_max(colIds[i]) != 1 )
          res[i] = RowVecTask.NORMALIZE_DATA;
      break;
    case STANDARDIZE:
      res = new int[colIds.length];
      for( int i = 0; i < colIds.length - 1; ++i )
        if( (ary.col_mean(colIds[i]) != 0) || ary.col_sigma(colIds[i]) != 1 )
          res[i] = RowVecTask.STANDARDIZE_DATA;
      break;
    case NONE:
      break;
    default:
      throw new Error("Invalid data preprocessing flag " + p);
    }
    return res;
  }

  public static GLM_Model solve(ValueArray ary, int[] colIds, Sampling s,
      GLM_Params glmParams, LSM_Params lsmParams, FamilyArgs fargs) {
    int[] p = null;
    if(lsmParams == null)lsmParams = defaultLSMParams;

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
      LSMTask tsk = new LSMTask(colIds, s, colIds.length - 1,  lsmParams.constant, p, ary.col_mean(colIds[colIds.length-1]));
      tsk.invoke(ary._key);
      m.n = tsk._n;
      m.beta = solveLSM(tsk._xx, tsk._xy, lsmParams);
      return m;
    }
    double [] beta = new double [colIds.length];
    Arrays.fill(beta, glmParams.link.defaultBeta);
    double diff;
    long N = 0;

    // do preprocessing!
    try{
      do {
        IRLSMTask tsk;
        switch(glmParams.family){
        case binomial:
          BinomialArgs bargs = (BinomialArgs)fargs;
          tsk = new BinomialTask(colIds, s, lsmParams.constant, beta, p, glmParams.link, ary.col_mean(colIds[colIds.length-1]),bargs);
          break;
        default:
          tsk = new IRLSMTask(colIds, s, lsmParams.constant, beta, p, glmParams.family, glmParams.link,ary.col_mean(colIds[colIds.length-1]));
        }
        diff = 0;
        tsk.invoke(ary._key);
        m.nullDeviance = tsk._nullDeviance;
        m.residualDeviance = tsk._deviance;
        N = tsk._n;
        tsk._beta = solveLSM(tsk._xx, tsk._xy, m.lsmParams);
        if( beta != null ) for( int i = 0; i < beta.length; ++i )
          diff = Math.max(diff, Math.abs(beta[i] - tsk._beta[i]));
        else diff = Double.MAX_VALUE;
        beta = tsk._beta;
      } while( diff > BETA_EPS );
    } catch (Exception e) {
      if(beta == null)throw new GLSMException("Failed to compute the data: " + e.getMessage());;
      m.warnings = new String[]{"Failed to converge"};
    }
    m.beta = beta;
    m.n = N;
    return m;
  }



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
    double     _constant; // constant member
    double     _ymu;     // mean(y) estimate
    double     _deviance;
    double _nullDeviance;

    public LSMTask() {
    } // Empty constructors for the serializers

    public LSMTask(int[] colIds, int constant, int[] p, double ymu) {
      this(colIds, null, colIds.length - 1, constant, p,ymu);
    }

    protected LSMTask(int[] colIds, Sampling s, int xlen, int constant, int[] p, double ymu) {
      super(colIds, s, true, p);
      _constant = constant;
      _ymu = ymu;
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
      // compute x*x' and add it to the marix
      for( int i = 0; i < (x.length - 1); ++i ) {
        for( int j = 0; j <= i; ++j ) { // matrix is symmetric -> compute only lower diag
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
    if( p == 1 || p == 0 ) return MAX_SQRT;
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
  public static class IRLSMTask extends LSMTask {
    double[] _beta;
    double   _origConstant;

    int      _f;
    int      _l;

    public IRLSMTask() {
    } // Empty constructor for the serializers

    public IRLSMTask(int[] colIds, Sampling s, int constant, double[] beta,
        int[] p, Family f, Link l, double ymu) {
      super(colIds, s, colIds.length - 1, constant, p,ymu);
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
//      _deviance += deviance(f,y,gmu);
//      double dev = deviance(f,y, _ymu);
//      _nullDeviance += dev;
      // get the inverse to get esitamte of p(Y=1|X) according to previous model
      double mu = linkInv(l,gmu);
      double dgmu = linkDeriv(l,mu);
      x[x.length - 1] = gmu + (y - mu) * dgmu; // z = y approx by Taylor
                                               // expansion at the point of our
                                               // estimate (mu), done to avoid
                                               // log(0),log(1)
      // Step 2
      double vary = variance(f,mu); // variance of y accrodgin to our model
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
      // compute the deviance according to the previous model

      super.map(x);
    }

    @Override
    public void reduce(DRemoteTask drt){
      IRLSMTask other = (IRLSMTask)drt;
      _nullDeviance += other._nullDeviance;
      _deviance += other._deviance;
      super.reduce(drt);
    }
  }

  public static class BinomialTask extends IRLSMTask {
    double _case; // in
    long _caseCount; // out

    public BinomialTask(int [] colIds, Sampling s, int constant, int [] p, Link l, double ymu, BinomialArgs bargs){
      this(colIds, s, constant, null, p, l, ymu, bargs);
    }
    public BinomialTask(int [] colIds, Sampling s, int constant, double [] beta, int [] p, Link l, double ymu, BinomialArgs bargs){
      super(colIds,s,constant, beta, p,  Family.binomial,l,ymu);
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
    boolean  _treatNonzerosAsOne;

    public LogitLSMTask() {
    } // Empty constructor for the serializers

    public LogitLSMTask(int[] colIds, Sampling s, int constant, double[] beta,
        int[] p, boolean nonZerosAsOnes) {
      super(colIds, s, colIds.length - 1, constant, p,0);
      _beta = beta;
      _treatNonzerosAsOne = nonZerosAsOnes;
    }

    public LogitLSMTask(int[] colIds, Sampling s, int constant, int[] p,
        boolean nonZerosAsOnes) {
      this(colIds, s, constant, new double[colIds.length
          - ((constant == 0) ? 1 : 0)], p, nonZerosAsOnes);
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
      if( _treatNonzerosAsOne && x[x.length - 1] != 0 ) x[x.length - 1] = 1;
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
      // get the inverse to get estimate of p(Y=1|X) according to previous model
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


    double[] _inputParams; // constant + beta
    double[] _results;

    double _ymu;
    int _familyOrd;
    int _linkOrd;
    transient Link _link;
    transient Family _family;

    public LSMTest() {
    }

    public LSMTest(GLM_Model m, Sampling s, double ymu) {
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
      LSMTest other = (LSMTest) drt;
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
