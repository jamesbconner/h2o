package hex;

import init.H2OSerializable;

import com.google.gson.JsonObject;

import Jama.CholeskyDecomposition;
import Jama.Matrix;

public final class LSMSolver implements H2OSerializable{

  static final int NO_PENALTY = 0;
  static final int L1_PENALTY = 1;
  static final int L2_PENALTY = 2;
  static final int EL_PENALTY = 3;

  int _penalty;

  public static final double DEFAULT_LAMBDA = 1e-5;
  public static final double DEFAULT_LAMBDA2 = 1e-8;
  public static final double DEFAULT_ALPHA = 1;
  public static final double DEFAULT_RHO = 1e-5;

  public boolean normalize() {
    return _penalty != NO_PENALTY;
  }

  public LSMSolver () {}
  public static LSMSolver makeSolver(){
    return new LSMSolver();
  }

  public static LSMSolver makeL2Solver(double lambda){
    LSMSolver res = new LSMSolver();
    res._penalty = L2_PENALTY;
    res._lambda = lambda;
    return res;
  }

  public static LSMSolver makeL1Solver(double lambda, double rho, double alpha){
    LSMSolver res = new LSMSolver();
    res._penalty = L1_PENALTY;
    res._lambda = lambda;
    res._rho = rho;
    res._alpha = alpha;
    return res;
  }

  public static LSMSolver makeElasticNetSolver(double lambda, double lambda2, double rho, double alpha){
    LSMSolver res = new LSMSolver();
    res._penalty = EL_PENALTY;
    res._lambda = lambda;
    res._lambda2 = lambda2;
    res._rho = rho;
    res._alpha = alpha;
    return res;
  }


  double _lambda = 0.0;
  double _lambda2 = 0.0;
  double _rho = 1e-5;
  double _alpha = 1.0;

  public JsonObject toJson(){
    JsonObject res = new JsonObject();
    switch(_penalty){
    case NO_PENALTY:
      res.addProperty("penalty", "none");
      break;
    case L2_PENALTY:
      res.addProperty("penalty","L2");
      res.addProperty("lambda",_lambda);
      break;
    case L1_PENALTY:
      res.addProperty("penalty","L1");
      res.addProperty("lambda",_lambda);
      res.addProperty("rho",_rho);
      res.addProperty("alpha",_alpha);
      break;
    case EL_PENALTY:
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
    case NO_PENALTY:
      lambda = 0.0;
      break;
    case L1_PENALTY:
      lambda = _rho;
      break;
    case L2_PENALTY:
      lambda = _lambda;
      break;
    case EL_PENALTY:
      lambda= _rho + _lambda2;
    default:
      throw new Error("unexpected penalty " + _penalty);
    }
    Matrix [] mm = m.getXandY(lambda);
    Matrix xx = mm[0];
    Matrix xy = mm[1];
    CholeskyDecomposition lu = new CholeskyDecomposition(xx);

    switch(_penalty) {
    case NO_PENALTY:
    case L2_PENALTY: // L2 and no penalty need only one iteration
      return lu.solve(xy).getColumnPackedCopy();
    case EL_PENALTY:
    case L1_PENALTY:  // use ADMM to solve LASSO
      final int N = xx.getRowDimension();
      final double ABSTOL = Math.sqrt(N) * 1e-4;
      final double RELTOL = 1e-2;
      double[] z = new double[N];
      double[] u = new double[N];
      Matrix xm = null;
      Matrix xyPrime = (Matrix)xy.clone();
      double kappa = _lambda / _rho;
      for( int i = 0; i < 10000; ++i ) {
        // first compute the x update
        // add rho*(z-u) to A'*y and add rho to diagonal of A'A
        for( int j = 0; j < N; ++j ) {
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
        for( int j = 0; j < N; ++j ) {
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
      return z;
   default:
     throw new Error("unexpected penalty " + _penalty);
    }
  }
}
