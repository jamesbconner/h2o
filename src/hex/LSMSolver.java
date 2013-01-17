package hex;

import water.Iced;
import Jama.CholeskyDecomposition;
import Jama.Matrix;

import com.google.gson.JsonObject;

public final class LSMSolver extends Iced {

  public static enum Norm { NONE, L1, L2, ELASTIC; }
  public Norm _penalty;

  public static final double DEFAULT_LAMBDA = 1e-5;
  public static final double DEFAULT_LAMBDA2 = 1e-5;
  public static final double DEFAULT_ALPHA = 1;
  public static final double DEFAULT_RHO = 1e-2;

  public boolean normalize() {
    return _penalty != Norm.NONE;
  }

  public LSMSolver () {
    _penalty = Norm.NONE;
  }

  public static LSMSolver makeSolver(){
    return new LSMSolver();
  }

  public static LSMSolver makeL2Solver(double lambda){
    LSMSolver res = new LSMSolver();
    res._penalty = Norm.L2;
    res._lambda = lambda;
    return res;
  }

  public static LSMSolver makeL1Solver(double lambda, double rho, double alpha){
    LSMSolver res = new LSMSolver();
    res._penalty = Norm.L1;
    res._lambda = lambda;
    res._rho = rho;
    res._alpha = alpha;
    return res;
  }

  public static LSMSolver makeElasticNetSolver(double lambda, double lambda2, double rho, double alpha){
    LSMSolver res = new LSMSolver();
    res._penalty = Norm.ELASTIC;
    res._lambda = lambda;
    res._lambda2 = lambda2;
    res._rho = rho;
    res._alpha = alpha;
    return res;
  }


  public double _lambda = 0.0;
  public double _lambda2 = 0.0;
  public double _rho = 1e-5;
  public double _alpha = 1.0;

  public JsonObject toJson(){
    JsonObject res = new JsonObject();
    switch(_penalty){
    case NONE:
      res.addProperty("penalty", "none");
      break;
    case L2:
      res.addProperty("penalty","L2");
      res.addProperty("lambda",_lambda);
      break;
    case L1:
      res.addProperty("penalty","L1");
      res.addProperty("lambda",_lambda);
      res.addProperty("rho",_rho);
      res.addProperty("alpha",_alpha);
      break;
    case ELASTIC:
      res.addProperty("penalty","L1 + L2");
      res.addProperty("lambda",_lambda);
      res.addProperty("lambda2",_lambda2);
      res.addProperty("rho",_rho);
      res.addProperty("alpha",_alpha);
    }
    return res;
  }

  private static double shrinkage(double x, double kappa) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  public double [] solve(GramMatrix m){
    double lambda;
    switch( _penalty ) {
    case NONE:    lambda = 0.0;              break;
    case L1:      lambda = _rho;             break;
    case L2:      lambda =        _lambda ;  break;
    case ELASTIC: lambda = _rho + _lambda2;  break;
    default:
      throw new IllegalArgumentException("unexpected penalty " + _penalty);
    }
    Matrix [] mm = m.getXandY(lambda);
    Matrix xx = mm[0];
    Matrix xy = mm[1];
    CholeskyDecomposition lu = new CholeskyDecomposition(xx);

    switch(_penalty) {
    case NONE:
    case L2: // L2 and no penalty need only one iteration
      return lu.solve(xy).getColumnPackedCopy();
    case ELASTIC:
    case L1:  // use ADMM to solve LASSO
      final int N = xx.getRowDimension();
      final double ABSTOL = Math.sqrt(N) * 1e-4;
      final double RELTOL = 1e-2;
      double[] z = new double[N-1];
      double[] u = new double[N-1];
      Matrix xm = null;
      Matrix xyPrime = (Matrix)xy.clone();
      double kappa = _lambda / _rho;

      for( int i = 0; i < 10000; ++i ) {
        // first compute the x update
        // add rho*(z-u) to A'*y
        for( int j = 0; j < N-1; ++j ) {
          xyPrime.set(j, 0, xy.get(j, 0) + _rho * (z[j] - u[j]));
        }

        // updated x
        xm = lu.solve(xyPrime);
        // vars to be used for stopping criteria
        double x_norm = 0;
        double z_norm = 0;
        double u_norm = 0;
        double r_norm = 0;
        double s_norm = 0;
        double eps_pri = 0; // epsilon primal
        double eps_dual = 0;
        // compute u and z update
        for( int j = 0; j < N-1; ++j ) {
          double x_hat = xm.get(j, 0);
          x_norm += x_hat * x_hat;
          x_hat = x_hat * _alpha + (1 - _alpha) * z[j];
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
        s_norm = _rho * Math.sqrt(s_norm);
        eps_pri = ABSTOL + RELTOL * Math.sqrt(Math.max(x_norm, z_norm));
        eps_dual = ABSTOL + _rho * RELTOL * Math.sqrt(u_norm);

        if( r_norm < eps_pri && s_norm < eps_dual ) break;
      }
      return xm.getColumnPackedCopy();
   default:
     throw new IllegalArgumentException("unexpected penalty " + _penalty);
    }
  }
}
