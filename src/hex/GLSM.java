package hex;

import java.io.*;
import java.util.*;

import water.*;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;
import Jama.Matrix;

/**
 * General Linear Regression (http://en.wikipedia.org/wiki/Linear_regression) implementation
 * based on least squared minimization.
 *
 * Solves problem of finding parameters bs  s.t. sum((bi*xi + b0 - y)^2) = min
 * (in other words, y is modeled as y' = bi*x + b0 such that the sum of squared errors (y - y')^2 is minized.
 * Bs are computed by following equation: (sum(x'*x)/n)^-1 * sum(x*y)/n, where:
 *    x is a (column) vector of input values
 *    x' is transposed x
 *    y is response variable
 *
 * This algorithm can be customized by providing Row2VecMap object, which turns raw data row into an x vector
 * and y response variable.  Eg. weights can be applied easily (on x) to do weighted linear regression.
 *
 * See logistic regression for an example of customized use of this algorithm.
 *
 * @author tomasnykodym
 *
 */
public class GLSM {

  public static class GLSMException extends RuntimeException {
    public GLSMException(String msg){super(msg);}
  }

  public enum Family {
    gaussian,
    binomial
  }

  protected static double [] solve (Key aryKey, LSMTask tsk){
    tsk.invoke(aryKey);
    try {
      tsk.get();
    } catch( Exception e ) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }
    Matrix xx;
    try {xx = new Matrix(tsk._xx).inverse();}catch(RuntimeException e){throw new GLSMException("can not perform LSM on this data, obtained matrix is singular!");}
    return xx.times(new Matrix(tsk._xy,tsk._xy.length)).getColumnPackedCopy();
  }


  public static double [] solve(Key aryKey, int [] colIds, int c, Family f) {
    switch(f){
    case gaussian:
      return solve(aryKey,new LSMTask(colIds, colIds.length-1,c));
    case binomial:
    {
      double [] oldBeta;
      double [] newBeta = solve(aryKey,new LogitLSMTask(colIds, c));
      double diff = 0;
      do{
        oldBeta = newBeta;
        newBeta = solve(aryKey,new LogitLSMTask(colIds, c, oldBeta));
        diff = 0;
        for(int i = 0; i < newBeta.length; ++i) diff += (oldBeta[i] - newBeta[i])*(oldBeta[i] - newBeta[i]);
      }while(diff > 1e-5);
      return newBeta;
    }
    default:
      throw new GLSMException("Unsupported family: " + f.toString());
    }

  }

  // wrapper around one row of data for use in WLR
  public static class Row {
    public Row(){}
    public Row(int n, int c){
      if (c != 0){
        x = new double [n+1];
        x[n] = c;
      } else
        x = new double [n];
    }

    public Row(Row r){
      x = r.x.clone();
      y = r.y;
    }
    // the x (row) vector
    public double [] x;

    public double  y;

    public String toString() {
      return "x = " + Arrays.toString(x)  + ", y = " + y;
    }
    @Override public Object clone(){
      return new Row(this);
    }
  }

  public static class LSMTask extends MRTask {
    double [][]    _xx;
    double []  _xy;
    int [] _colIds; // indexes of the columns we're interested in, first n-1 columns are for x vector, the last column is for y
    int _xlen;
    int _constant;
    private Row _r;

    public LSMTask(int [] colIds, int constant) {
      this(colIds, colIds.length-1,constant);
    }
    protected LSMTask(int [] colIds, int xlen, int constant) {
      _colIds = colIds;
      _xlen = xlen;
      _constant = constant;
    }

    protected Row getRow(int rid, ValueArray ary, byte [] bits, int row_size, int [] off, int [] sz, int [] base, int [] scale){
      if(_r == null )_r = new Row(_xlen, _constant);
      int y = _colIds.length-1;
      for(int i = 0; i < y; ++i)
        _r.x[i] = ary.datad(bits, rid, row_size, off[i], sz[i], base[i], scale[i], _colIds[i]);
      _r.y = ary.datad(bits, rid, row_size, off[y], sz[y], base[y], scale[y], _colIds[y]);
      return _r;
    }
    /**
     * Body of the LR, computes sum(x'*x)/n and sum(x*y)/n for all rows in this chunk.
     */
    @Override
    public void map(Key key) {
      assert key.home();
      Key aryKey = Key.make(ValueArray.getArrayKeyBytes(key));
      ValueArray ary = (ValueArray) DKV.get(aryKey);
      byte[] bits = DKV.get(key).get();
      _xy = new double[_xlen];
      _xx = new double[_xlen][_xlen];
      int [] off = new int [_colIds.length];
      int [] sz = new int [_colIds.length];
      int [] base = new int [_colIds.length];
      int [] scale = new int [_colIds.length];
      for(int i = 0; i < _colIds.length; ++i){
        off[i] = ary.col_off(_colIds[i]);
        sz[i] = ary.col_size(_colIds[i]);
        base[i] = ary.col_base(_colIds[i]);
        scale[i] = ary.col_scale(_colIds[i]);
      }
      // _rmap gets shared among threads ->  create thread's private copy
      int row_size = ary.row_size();
      int nrows = bits.length / row_size;
      for(int rid = 0; rid < nrows; ++rid) {
        Row r = getRow(rid,ary,bits, row_size, off, sz, base, scale);
        for(int i = 0; i < _xlen; ++i){
          for(int j = 0; j < _xlen; ++j){
            _xx[i][j] += r.x[i]*r.x[j];
          }
          _xy[i] += r.x[i]*r.y;
        }
      }
      double nInv = 1 / (double) ary.num_rows();
      for(int i = 0; i < _xlen; ++i){
        for(int j = 0; j < _xlen; ++j){
          _xx[i][j] /=  nInv;
        }
        _xy[i] /= nInv;
      }
      // do not pass this back!
      _colIds = null;
    }

    /**
     * Add partial results.
     */
    @Override
    public void reduce(DRemoteTask drt) {
      LSMTask other = (LSMTask) drt;
      if(_xx != null || _xy != null) {
        for(int i = 0; i < _xx.length; ++i) {
          for(int j = 0; j < _xx.length; ++j) {
            _xx[i][j] += other._xx[i][j];
          }
          _xy[i] += other._xy[i];
        }
      } else {
        _xx = other._xx;
        _xy = other._xy;
      }
    }

  }

  public static class LogitLSMTask extends LSMTask {
    double [] _beta;

    public LogitLSMTask(int [] colIds, int constant, double [] beta) {
      super(colIds, colIds.length-1,constant);
      _beta = beta;
    }

    public LogitLSMTask(int [] colIds, int constant) {
      this(colIds,constant, new double[colIds.length - ((constant == 0)?1:0)]);
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
    protected Row getRow(int rid, ValueArray ary, byte [] bits, int row_size, int [] off, int [] sz, int [] base, int [] scale){
      Row r = super.getRow(rid, ary, bits, row_size, off, sz, base, scale);
      assert 0 <= r.y && r.y <= 1;
      // transform input to the GLR according to Olga's slides
      // (glm lecture, page 12)
      // Step 1
      double gmu = 0.0;
      for(int i = 0; i < r.x.length; ++i){
        gmu += r.x[i] * _beta[i];
      }
      double mu = gInv(gmu);
      // Step 2
      double vary = mu * (1 - mu);
      double dgmu = gPrime(mu);
      r.y = gmu + (r.y - mu)*dgmu;
      // compuet the weights (inverse of variance of z)
      double var = dgmu*dgmu*vary;
      // apply the weight, we want each datapoint to have weight of inverse of the variance of y at this point.
      // since we compute x'x, we take sqrt(w) and apply it to both x and y (since we also compute X*y)
      double w = Math.sqrt(1/var);
      for(int i = 0; i < r.x.length; ++i)
        r.x[i] *= w;
      r.y *= w;
      // step 3 performed by GLR
      return r;
    }
  }
}
