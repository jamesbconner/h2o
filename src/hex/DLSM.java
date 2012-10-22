package hex;

import hex.DGLM.DataPreprocessing;
import hex.DGLM.Norm;
import water.DRemoteTask;
import water.ValueArray;
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

  // ADMM shrinkgge operator
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
   */
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

    // Cholesky decomposition is faster than matrix inverse. Also, for L1 solution, we can cache the result.
    CholeskyDecomposition lu = new CholeskyDecomposition(xx);
    switch( params.n ) {
    case NONE:
    case L2: // L2 and no penalty need only one iteration
      return lu.solve(new Matrix(xyAry, xyAry.length)).getColumnPackedCopy();
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
   * Return the vector with data preprocessing flags. Will mark the columns which need to be normalized/regularized.
   *
   * If DataPReprocessing is set to AUTO, it will be set to Regularization if norm = L1 or L2.
   * For each column, test if it already is regularized/normalized and set the flag accordingly.
   *
   * @param ary
   * @param colIds
   * @param p
   * @param n
   * @return array of preprocessing flags
   */
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
}
