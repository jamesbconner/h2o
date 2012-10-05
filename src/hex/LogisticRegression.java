package hex;

import hex.GLinearRegression.GLRException;
import hex.GLinearRegression.Row;
import water.*;
import water.web.H2OPage;
import Jama.Matrix;

/**
 * Logistic regression implementation, using iterative least squares method.
 *
 * Basically repeatedly calls GLR with custom input transformation until the fixpoint of the function is reached.
 * To compute the logistic regression using GLR, following operations are performed on the input:
 *
 *
 * @author tomasnykodym
 *
 */
public class LogisticRegression {
  public static class LogitMap extends GLinearRegression.LinearRow2VecMap{
    Matrix _beta;
    public LogitMap(){}
    public LogitMap(int[] xColIds, int yColId) {
      this(xColIds,yColId,new Matrix(xColIds.length+1,1));
    }
    public LogitMap(LogitMap other){
      super(other);
      _beta = (Matrix)other._beta.clone();
    }

    public LogitMap(int[] xColIds, int yColId, Matrix initialGuess) {
      super(xColIds, yColId);
      // make sure weighted x is separate object
      _beta = initialGuess;
    }

    @Override public LogitMap clone(){
      return new LogitMap(this);
    }

    double gPrime(double p){
      if(p == 1 ||  p == 0) return 0;
      return 1 / (p*(1-p));
    }

    double g(double x){
      return Math.log(x)/Math.log(1-x);
    }

    double gInv(double x){
      return 1.0/(Math.exp(-x)+1.0);
    }

    @Override
    public Row map(int rid){
      Row r =  super.map(rid);
      assert 0 <= r.y && r.y <= 1;
      // transform input to the GLR according to Olga's slides
      // (glm lecture, page 12)
      // Step 1
      Matrix x = r.x.times(_beta);
      assert x.getRowDimension() == x.getColumnDimension();
      assert x.getRowDimension() == 1;
      double gmu = r.x.times(_beta).get(0, 0);
      double mu = gInv(gmu);
      // Step 2
      double vary = mu * (1 - mu);
      double dgmu = gPrime(mu);
      r.y = gmu + (r.y - mu)*dgmu;
      // compuet the weights (inverse of variance of z)
      double var = dgmu*dgmu*vary;
      r.wx.timesEquals(1/var);
      // step 3 performed by GLR
      return r;
    }

    @Override
    public int wire_len() {
      return super.wire_len()+_beta.getRowDimension()*8;
    }

    @Override
    public void read(Stream s) {
      super.read(s);
      _beta = (Matrix)_row.wx.clone();
      for(int i = 0; i < _beta.getRowDimension(); ++i)
        _beta.set(i,0, s.get8d());
    }
    @Override
    public void write(Stream s) {
      super.write(s);
      for(int i = 0; i < _beta.getRowDimension(); ++i)
        s.set8d(_beta.get(i,0));
    }
  }


  public static double [] web_main(Key aryKey, int [] xColIds, int yColId){
    return solve(aryKey,xColIds,yColId).getColumnPackedCopy();
  }

  public static Matrix solve(Key aryKey, int [] xColIds, int yColId) {
    // initial guess
    Matrix oldBeta = new Matrix(xColIds.length+1,1);
    // initial guess is all 1s
    Matrix newBeta = GLinearRegression.solveGLR(aryKey, new LogitMap(xColIds, yColId,oldBeta));
    Matrix diff = newBeta.minus(oldBeta);
    double norm = diff.transpose().times(diff).get(0, 0);

    while(norm > 1e-5){
      //System.out.println("beta = " + newBeta.transpose());
      oldBeta = newBeta;
      newBeta = GLinearRegression.solveGLR(aryKey, new LogitMap(xColIds, yColId,oldBeta));
      for(int i = 0; i < newBeta.getRowDimension(); ++i){
        if(Double.isInfinite(newBeta.get(i,0)) || Double.isNaN(newBeta.get(i,0))){
          System.err.println("[LogisticRegression] got invalid beta during iteration, returning previous value");
          return oldBeta;
        }
      }
      diff = newBeta.minus(oldBeta);
      //System.out.println("beta = " + newBeta.transpose() + ", diff = "  + diff);
      norm = diff.transpose().times(diff).get(0, 0);
    }
    return newBeta;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

}
