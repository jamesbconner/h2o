package hex;

import java.util.Arrays;

import init.H2OSerializable;
import Jama.Matrix;

public final class GramMatrix implements H2OSerializable {
  int _n;
  private double [][] _xx;
  private double [] _xy;

  public GramMatrix(int n) {
    _n = n;
    _xx = new double [n][];
    _xy = new double [n];
    for(int i = 0; i < n; ++i)
      _xx[i] = new double[i+1];
  }

  public void addRow(double [] x, int [] indexes, double y){
    for(int i = 0; i < x.length; ++i){
      for(int j = 0; j < x.length; ++j){
        if(indexes[j] > indexes[i])break;
        _xx[indexes[i]][indexes[j]] += x[i]*x[j];
      }
      _xy[indexes[i]] += x[i] * y;
    }

  }

  public void add(GramMatrix other){
    assert _n == other._n:"trying to merge incompatible gram matrices";
    for(int i = 0; i < _xx.length; ++i) {
      _xy[i] += other._xy[i];
      for(int j = 0; j < _xx[i].length; ++j)
        _xx[i][j] += other._xx[i][j];
    }
  }

  public Matrix [] getXandY(double lambda){
    Matrix xx = new Matrix(_xx.length, _xx.length);
    for( int i = 0; i < _xx.length; ++i ) {
      for( int j = 0; j < _xx[i].length; ++j ) {
        if(i == j) {
          xx.set(i, j, _xx[i][j] + ((i != _n)?lambda:0.0)); // constant is not regularized
        } else {
          xx.set(i, j, _xx[i][j]);
          xx.set(j, i, _xx[i][j]);
        }
      }
    }
    return new Matrix[]{xx,new Matrix(_xy, _xy.length)};
  }

}
