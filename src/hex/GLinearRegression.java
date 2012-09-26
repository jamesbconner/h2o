package hex;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
public class GLinearRegression {

  /**
   * Abstract class used to customize the algorithm. Responsible for turning raw input data rows
   * into x (variable vector) and y (response variable, scalar).
   *
   * Provides interface for serialization/deserialization and iteration over the rows (in a chunk).
   * To customize, override method map(int rowId) to compute x and y based on the input data.
   *
   * @author tomasnykodym
   *
   */
  public static abstract class Row2VecMap implements Iterable<Row>, Cloneable {
    ValueArray _ary;
    byte[]     _bits;
    int        _row_size;

    public static Row2VecMap deserialize(byte[] buff, int off) {
      try {
        Class<? extends Row2VecMap> c = (Class<? extends Row2VecMap>) Class
            .forName(new String(buff, off + 1, buff[off]));
        ;
        Row2VecMap m = c.newInstance();
        m.read(buff, off + buff[off] + 1);
        return m;
      } catch( Exception e ) {
        throw new Error(e);
      }
    }
    // helper function to serialize this, puts the classname first, data follow
    public static int serialize(Row2VecMap r, byte[] buf, int off) {
      byte[] cb = r.getClass().getName().getBytes();
      assert ((byte) cb.length) == cb.length;
      buf[off++] = (byte) cb.length;
      System.arraycopy(cb, 0, buf, off, cb.length);
      off += cb.length;
      return r.write(buf, off);
    }

    // set the chunk we want to iterate over
    public void setRawData(ValueArray ary, byte[] bits) {
      _ary = ary;
      _bits = bits;
      _row_size = ary.row_size();
    }

    double getColumn(int r, int c) {
      return _ary.datad(_bits, r, _row_size, c);
    }

    public Iterator<Row> iterator() {
      final int nrows = (_bits != null && _row_size != 0) ? _bits.length
          / _row_size : 0;
      return new Iterator<Row>() {
        int _id = 0;

        @Override
        public boolean hasNext() {
          return _id < nrows;
        }

        @Override
        public Row next() {
          if( hasNext() ) return map(_id++);
          throw new NoSuchElementException();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
    public abstract Row2VecMap clone();
    public abstract Row map(int rid);
    public abstract int xlen();
    public abstract int wire_len();
    public abstract int read(byte[] buff, int off);
    public abstract void read(DataInputStream istream);
    public abstract int write(byte[] buff, int off);
    public abstract void write(DataOutputStream ostream);
  }

  /**
   * Basic implementation of Row2VecMap. Selects multiple columns for x, single column for y.
   * No data transformations.
   *
   * @author tomasnykodym
   */
  public static class LinearRow2VecMap extends Row2VecMap {
    int[] _xs;
    int   _y;
    int _constant;
    Row   _row;
    int   _xlen;

    public LinearRow2VecMap(){}
    public LinearRow2VecMap(int[] xColIds, int yColId) {
      this(xColIds, yColId, 1);
    }

    public LinearRow2VecMap(LinearRow2VecMap other){
      _xs = other._xs.clone();
      _y = other._y;
      _constant = other._constant;
      _row = (Row)other._row.clone();
      _xlen = other._xlen;
    }

    public LinearRow2VecMap(int[] xColIds, int yColId, int constant) {
      _xs = xColIds;
      _y = yColId;
      _row = new Row();
      _xlen = (constant != 0) ? _xs.length + 1 : _xs.length;
      _row.x = new Matrix(1,_xlen);
      _constant = constant;
      if( constant != 0 ) _row.x.set(0,_xs.length, constant);
      _row.wx = _row.x.transpose();
      _row.y = 0.0;
    }

    @Override public LinearRow2VecMap clone(){
      return new LinearRow2VecMap(this);
    }

    @Override
    public Row map(int rid) {
      for( int i = 0; i < _xs.length; ++i ) {
        double d = getColumn(rid, _xs[i]);
        _row.x.set(0,i,d);
        _row.wx.set(i,0,d);
      }
      if(_constant != 0){ // make sure constants are ok
        _row.x.set(0,_row.x.getColumnDimension()-1,_constant);
        _row.wx.set(_row.wx.getRowDimension()-1,0,_constant);
      }
      _row.y = getColumn(rid, _y);
      return _row;
    }

    @Override
    public int xlen() {
      return _row.wx.getRowDimension();
    }

    @Override
    public int wire_len() {
      return (1 + _xs.length + 1 + 1) << 2; // xs.length + xs + y + constant
    }

    @Override
    public int read(byte[] buf, int off) {
      _xs = new int [UDP.get4(buf,off)]; off += 4;
      for(int i = 0; i < _xs.length; ++i){
        _xs[i] = UDP.get4(buf, off); off += 4;
      }
      _y = UDP.get4(buf, off); off += 4;
      _constant = UDP.get4(buf, off);
      _row = new Row();
      _xlen = (_constant != 0) ? _xs.length + 1 : _xs.length;
      _row.x = new Matrix(1,_xlen);
      if(_constant != 0 ) _row.x.set(0,_xs.length, _constant);
      _row.wx = _row.x.transpose();
      _row.y = 0.0;
      return off + 4;
    }

    @Override
    public int write(byte[] buf, int off) {
      UDP.set4(buf, off, _xs.length);
      off += 4;
      for( int x : _xs ) {
        UDP.set4(buf, off, x);
        off += 4;
      }
      UDP.set4(buf, off, _y); off += 4;
      UDP.set4(buf, off, _constant);
      return off + 4;
    }

    @Override
    public void write(DataOutputStream ostream) {
      throw new RuntimeException("TODO Auto-generated method stub");
    }

    @Override
    public void read(DataInputStream istream) {
      throw new RuntimeException("TODO Auto-generated method stub");
    }

  }

  public static Matrix solveGLR(Key aryKey, int [] xColIds, int yColId) {
    return solveGLR(aryKey, new LinearRow2VecMap(xColIds, yColId));
  }

  public static Matrix solveGLR(Key aryKey, GLinearRegression.Row2VecMap rmap) {
    GLRTask tsk = new GLRTask(rmap);
    tsk.invoke(aryKey);
    try {
      tsk.get();
    } catch( Exception e ) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }
    return tsk._xx.inverse().times(tsk._xy);
  }

  // wrapper around one row of data for use in WLR
  public static class Row {
    public Row(){}
    public Row(Row r){
      x = (Matrix)r.x.clone();
      wx = (Matrix)r.wx.clone();
      y = r.y;
    }
    // the x (row) vector
    public Matrix x;
    // weighted (column) x (or just alias to x, if weights are not used)
    public Matrix wx;
    // response variable
    public double  y;

    public String toString() {
      return "x = " + x + ", wx = " + wx + ", y = " + y;
    }
    @Override public Object clone(){
      return new Row(this);
    }
  }

  @RTSerializer(GLRTask.Serializer.class)
  public static class GLRTask extends MRTask {
    Matrix    _xx;
    Matrix    _xy;
    Row2VecMap _rmap;

    public GLRTask(Row2VecMap rmap) {
      _rmap = rmap.clone();
    }


    // private constructor to be used in deserialization of results
    private GLRTask(Matrix xx, Matrix xy) {
      _xx = xx;
      _xy = xy;
      _rmap = null;
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
      int xlen = _rmap.xlen();
      _xy = new Matrix(xlen,1);
      _xx = new Matrix(xlen,xlen);
      // _rmap gets shared among threads ->  create thread's private copy
      Row2VecMap rmap = _rmap.clone();
      rmap.setRawData(ary, bits);
      for( Row r : rmap ) {
        _xx.plusEquals(r.wx.times(r.x));
        r.wx.timesEquals(r.y);
        _xy.plusEquals(r.wx);
      }
      double nInv = 1 / (double) ary.num_rows();
      // multiply  by 1/n (so that the matrix value does not grow too much if the data is BIG)
      _xy.timesEquals(nInv);
      _xx.timesEquals(nInv);
    }

    /**
     * Add partial results.
     */
    @Override
    public void reduce(DRemoteTask drt) {
      GLRTask other = (GLRTask) drt;
      if(_xx != null || _xy != null) {
        _xx.plusEquals(other._xx);
        _xy.plusEquals(other._xy);
      } else {
        _xx = other._xx;
        _xy = other._xy;
      }
    }


    public static class Serializer extends RemoteTaskSerializer<GLRTask> {

      @Override
      public int wire_len(GLRTask task) {
        if( task._xx == null && task._xy == null ) {
          // initial stage, data not computed yet, pass down the xs and y
          return 1 + task._rmap.wire_len();
        } else { // already computed data, hand them back
          return 1 + ((task._xx.getColumnDimension() * task._xx.getRowDimension() + task._xy
              .getRowDimension()) << 3);
        }
      }

      @Override
      public int write(GLRTask task, byte[] buf, int off) {
        if( task._xx == null && task._xy == null ) {
          buf[off++] = 1; // state 1
          off = Row2VecMap.serialize(task._rmap, buf, off);
        } else {
          buf[off++] = 2; // state 2
          assert task._xx.getColumnDimension() == task._xx.getRowDimension();
          assert task._xy.getRowDimension() == task._xx.getRowDimension();
          UDP.set4(buf, off, task._xx.getRowDimension());
          off += 4;
          int M = task._xx.getRowDimension();
          int N = task._xx.getColumnDimension();
          for( int i = 0; i < M; ++i ) {
            for( int j = 0; j < N; ++j ) {
              UDP.set8d(buf, off, task._xx.get(i, j));
              off += 8;
            }
          }
          for( int i = 0; i < N; ++i ) {
            UDP.set8d(buf, off, task._xy.get(i,0));
            off += 8;
          }
        }
        return off;
      }

      @Override
      public GLRTask read(byte[] buf, int off) {
        switch( buf[off++] ) {
        case 1:
          return new GLRTask(Row2VecMap.deserialize(buf, off));
        case 2:
          int xlen = UDP.get4(buf, off);
          off += 4;
          Matrix xx = new Matrix(xlen, xlen);
          for( int i = 0; i < xlen; ++i ) {
            for( int j = 0; j < xlen; ++j ) {
              xx.set(i, j, UDP.get8d(buf, off));
              off += 8;
            }
          }
          Matrix xy = new Matrix(xlen,1);
          for( int i = 0; i < xlen; ++i ) {
            xy.set(i,0, UDP.get8d(buf, off));
            off += 8;
          }
          return new GLRTask(xx, xy);
        default:
          throw new Error("illegal data");
        }
      }

      @Override
      public void write(GLRTask task, DataOutputStream dos) throws IOException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }

      @Override
      public GLRTask read(DataInputStream dis) throws IOException {
        throw new RuntimeException("TODO Auto-generated method stub");
      }
    }
  }
}
