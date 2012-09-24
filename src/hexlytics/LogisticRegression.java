package hexlytics;

import hexlytics.GLinearRegression.Row;

import javax.vecmath.GVector;

import water.Key;
import water.UDP;

public class LogisticRegression {
  public static class LogitMap extends GLinearRegression.LinearRow2VecMap{
    GVector _beta;
    public LogitMap(int[] xColIds, int yColId, GVector initialGuess) {
      super(xColIds, yColId);
      // make sure weighted x is separate object
      _row.wx = (GVector)_row.x.clone();
    }

    double logitDerivative(double p){
      return 1 / (p*(1-p));
    }

    @Override
    public Row map(int rid){
      Row r =  super.map(rid);
      assert 0 <= r.y && r.y <= 1;
      // transform input to the GLR according to Olga's slides
      // (glm lecture, page 12)
      // Step 1
      double gmu = r.x.dot(_beta);
      double mu = 1/(Math.exp(-gmu)+1);
      // Step 2
      double vary = mu * (1 - mu);
      double dgmu = logitDerivative(mu);
      r.y = gmu + (r.y - mu)*dgmu;
      // compuet the weights (inverse of variance of z)
      double var = dgmu*dgmu*vary;
      r.wx.scale(1/var,r.x);
      // step 3 performed by GLR
      return r;
    }

    @Override
    public int read(byte[] buf, int off) {
      off = super.read(buf, off);
      _beta = new GVector(_row.x.getSize());
      for(int i = 0; i < _beta.getSize(); ++i){
        _beta.setElement(i, UDP.get8d(buf, off)); off += 8;
      }
      return off;
    }
    @Override
    public int write(byte[] buf, int off) {
      off = super.write(buf, off);
      for(int i = 0; i < _beta.getSize(); ++i){
        UDP.set8d(buf, off, _beta.getElement(i)); off += 8;
      }
      return off;
    }
  }

  public static GVector solve(Key aryKey, int [] xColIds, int yColId) {
    // initial guess
    GVector oldBeta = new GVector(xColIds.length+1);
    // initial guess is all 1s
    for(int i = 0; i < oldBeta.getSize(); ++i)oldBeta.setElement(i, 1.0);
    GVector newBeta = GLinearRegression.solveGLR(aryKey, new LogitMap(xColIds, yColId,oldBeta));
    GVector diff = (GVector)newBeta.clone();
    diff.sub(oldBeta);
    while(diff.norm() > 1e-5){
      oldBeta = newBeta;
      newBeta = GLinearRegression.solveGLR(aryKey, new LogitMap(xColIds, yColId,oldBeta));
      diff.scale(1, newBeta);
      diff.sub(oldBeta);
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
