package hex;

import java.util.Arrays;

import com.google.gson.JsonObject;
import water.DRemoteTask;
import Jama.CholeskyDecomposition;
import Jama.Matrix;


/**
 * Class for Distributed Least Square Fitting.
 *
 * Used to solve problem
 *     min_x Ax = b + f(x), where:
 *         A is a column matrix of data points,
 *         b is a response variable vector,
 *         x is the parameter vector,
 *         f(x) is penalty function.
 * Penalty function is optional and defaults to none (ie. f(x) = 0 for all x).
 * Currently  2 penalty functions are supported: L2_norm (ridge regression) and L1_norm(LASSO) penalty function .
 *
 * Least square fitting is currently done by solving the equation x = inv(A'A)*Ab and *will* fail if A'A is singular matrix
 * (happens when some of the columns are correlated).
 *
 *  The algorithm is implemented by running MR task to compute A'A and Ab (both fully distributed, 1 pass over data)
 *  and than solve x = inv(A'A)*Ab on a single node (and in single thread). The assumption here is that number of columns
 *  is small (< 1000) and so inv(A'A) can be computed easily.
 *  Number of rows is expected to be large and is practically unlimited.
 *
 *  If L1 or L2 norm are used, data is regularized (mean = 0, var = 1) in flight.
 *  Also, there is *NO* singularity problems if using L1 or L2 penalty function.
 *
 *  Limitations:
 *    1) number of columns < 1000
 *    2) will not solve for datasets with correlated columns if no penalty function is used (singularity issue).
 *    3) numerical stability: used method is not particularly numerically stable and can cause problems for tricky datasets.
 *
 *  Future improvements:
 *    a) implement distributed QR factorization for more stable computation (no singularity issues, much slower).
 *    b) Use ADMM to handle datasets with many columns.
 *      (Datasets can be distributed into smaller datasets and solved in parallel iteratively until they agree on the result).
 *    c) more penalty functions (Hubbert fitting, elastic net)
 *
 * @author tomasnykodym
 *
 */
public class DLSM {

  static class DLSM_SingularMatrixException extends RuntimeException {
    public double [] res;
    DLSM_SingularMatrixException(double [] r){res = r;}
  }

  // supported norms (penalty functions)
  public static enum Norm {
    NONE, // standard regression without any regularization
    L1,   // LASSO
    L2,
    ENET,
    ;   // ridge regression
  }

  public static class LSM_Params{
    public Norm n = Norm.NONE;
    public double lambda = 0;
    public double lambda2 = 0;
    public double rho = 0;
    public double alpha = 0;
    public int constant = 1;

    public LSM_Params(){}
    public LSM_Params(Norm n, double lambda, double rho, double alpha, int constant){
      this.n = n;
      this.lambda = lambda;
      this.rho = rho;
      this.alpha = alpha;
      this.constant = constant;
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("Norm", n.toString());
      res.addProperty("lambda", lambda);
      res.addProperty("lambda2", lambda2);
      res.addProperty("rho", rho);
      res.addProperty("alpha", alpha);
      res.addProperty("constant",constant);
      return res;
    }
  }

  // ADMM shrinkage operator
  private static double shrinkage(double x, double kappa) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  /**
   * Solves LSM given the A'A and Ab matrices.
   *
   * @param xxAry lower diagonal of A'A (A'A is symmetric)
   * @param xyAry Ab
   * @param params
   * @return x vector minimizing the given LSM problem.
   * @throws DLSM_SingularMatrixException
   */
  protected static double[] solveLSM(double[][] xxAry, double[] xyAry,
      LSM_Params params) throws DLSM_SingularMatrixException {
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
    case ENET:
      d = params.lambda2 + params.rho;
      break;
    default:
      assert false:"unexpected norm " + params.n;
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

    // Cholesky decomposition is faster than matrix inverse. Also, for L1 solution, we can cache the result.
    CholeskyDecomposition lu = new CholeskyDecomposition(xx);
    switch( params.n ) {
    case NONE:
    case L2: // L2 and no penalty need only one iteration
      try {
        return lu.solve(new Matrix(xyAry, xyAry.length)).getColumnPackedCopy();
      } catch(Exception e){
        params.n = Norm.L2;
        params.lambda = (params.lambda == 0)?1e-10:(10*params.lambda);
        if(params.lambda > 1e-5) {
          double [] res = new double[xxAry.length];
          Arrays.fill(res, Double.NaN);
          throw new DLSM_SingularMatrixException(res);
        }
        double [] res = solveLSM(xxAry, xyAry, params);
        throw new DLSM_SingularMatrixException(res);
      }
    case ENET:
    case L1: { // use ADMM to solve LASSO
      final int N = xyAry.length;
      final double ABSTOL = Math.sqrt(N) * 1e-4;
      final double RELTOL = 1e-2;
      double[] z = new double[N];
      double[] u = new double[N];
      Matrix xm = null;
      double kappa = params.lambda / params.rho;
      for( int i = 0; i < 10000; ++i ) {
        // first compute the x update
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
        double r_norm = 0;
        double s_norm = 0;
        double eps_pri = 0; // epsilon primal
        double eps_dual = 0;
        // compute u and z update
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
        // compute variables used for stopping criterium
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
  /**
   * MR task to compute A'A and Ab matrices in one pass.
   * Since A'A is symmetric, only lower diagonal is computed.
   * results are divided by N to avoid numerical problems with large dataset.
   * (loosing precision due to very high numbers).
   *
   * @author tomasnykodym
   *
   */
  public static class LSMTask extends RowVecTask {
    double[][] _xx;      // matrix holding sum of x*x'
    double[]   _xy;      // vector holding sum of x * y
    double     _constant; // constant member

    public LSMTask() {
    } // Empty constructors for the serializers

    public LSMTask(int[] colIds, int constant, double[][] pVals) {
      this(colIds, null, colIds.length - 1, constant, pVals);
    }

    protected LSMTask(int[] colIds, Sampling s, int xlen, int constant, double[][] pVals) {
      super(colIds, s, true, pVals);
      _constant = constant;
    }

    @Override
    public void preMap(int xlen, int nrows) {
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
    public void processRow(double[] x) {
      double y = x[x.length - 1];
      // compute x*x' and add it to the matrix
      for( int i = 0; i < (x.length - 1); ++i ) {
        for( int j = 0; j <= i; ++j ) { // matrix is symmetric -> compute only lower diag
          _xx[i][j] += x[i] * x[j];
        }
        _xy[i] += x[i] * y;
      }
      // compute the constant (constant is not part of x and has to be computed
      // separately)
      for( int j = 0; j < (x.length - 1); ++j )
        _xx[x.length - 1][j] += _constant * x[j];
      _xx[x.length - 1][x.length - 1] += _constant * _constant;
      _xy[x.length - 1] += _constant * y;
    }

    @Override
    public void postMap() {
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
    }

    /**
     * Add partial results.
     */
    @Override
    public void reduce(DRemoteTask drt) {
      LSMTask other = (LSMTask) drt;
      if( _xx != null || _xy != null ) {
        double myR = 0.5;
        double otherR = 0.5;
        if(_n != other._n){
          double R = 1.0 / (_n + other._n);
          myR = _n * R;
          otherR = other._n * R;
        }
        for( int i = 0; i < _xx.length; ++i ) {
          for( int j = 0; j <= i; ++j ) {
            _xx[i][j] = (myR * _xx[i][j] + otherR * other._xx[i][j]);
          }
          _xy[i] = (myR * _xy[i] + otherR * other._xy[i]);
        }
      } else {
        _xx = other._xx;
        _xy = other._xy;
      }
      _n += other._n;
    }
  }
}
