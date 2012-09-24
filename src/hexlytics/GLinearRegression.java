package hexlytics;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.vecmath.GMatrix;
import javax.vecmath.GVector;

import water.*;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

public class GLinearRegression {

  public static abstract class Row2VecMap implements Iterable<Row> {
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

    public static int serialize(Row2VecMap r, byte[] buf, int off) {
      byte[] cb = r.getClass().getName().getBytes();
      assert ((byte) cb.length) == cb.length;
      buf[off++] = (byte) cb.length;
      System.arraycopy(cb, 0, buf, off, cb.length);
      off += cb.length;
      return r.write(buf, off);
    }

    public void setRawData(ValueArray ary, byte[] bits) {
      _ary = ary;
      _bits = bits;
      _row_size = ary.row_size();
    }

    public double getColumn(int r, int c) {
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
    public abstract Row map(int rid);
    public abstract int xlen();
    public abstract int wire_len();
    public abstract int read(byte[] buff, int off);
    public abstract void read(DataInputStream istream);
    public abstract int write(byte[] buff, int off);
    public abstract void write(DataOutputStream ostream);
  }

  public static class LinearRow2VecMap extends Row2VecMap {
    int[] _xs;
    int   _y;
    int _constant;
    Row   _row;
    int   _xlen;

    public LinearRow2VecMap(int[] xColIds, int yColId) {
      this(xColIds, yColId, 1);
    }

    public LinearRow2VecMap(int[] xColIds, int yColId, int constant) {
      _xs = xColIds;
      _y = yColId;
      _row = new Row();
      _xlen = (constant != 0) ? _xs.length + 1 : _xs.length;
      _row.x = new GVector(_xlen);
      _constant = constant;
      if( constant != 0 ) _row.x.setElement(_xs.length, constant);
      _row.wx = _row.x;
      _row.y = 0.0;
    }

    @Override
    public Row map(int rid) {
      for( int i = 0; i < _xs.length; ++i )
        _row.x.setElement(i, getColumn(rid, _xs[i]));
      _row.y = getColumn(rid, _y);
      if(_constant != 0){
        _row.x.setElement(_row.x.getSize()-1,_constant);
        _row.wx.setElement(_row.wx.getSize()-1,_constant);
      }
      return _row;
    }

    @Override
    public int xlen() {
      return _row.x.getSize();
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

  public static GVector solveGLR(Key aryKey, int [] xColIds, int yColId) {
    return solveGLR(aryKey, new LinearRow2VecMap(xColIds, yColId));
  }

  public static GVector solveGLR(Key aryKey, GLinearRegression.Row2VecMap rmap) {
    GLRTask tsk = new GLRTask(rmap);
    tsk.invoke(aryKey);
    tsk._xx.invert();
    GVector betas = (GVector) tsk._xy.clone();
    betas.mul(tsk._xx, tsk._xy);
    return betas;
  }

  // wrapper around one row of data for use in WLR
  public static class Row {
    // the x vector
    public GVector x;
    // weighted x (or just alias to x, if weights are not used)
    public GVector wx;
    // response variable
    public double  y;

    public String toString() {
      return "x = " + x + ", wx = " + wx + ", y = " + y;
    }
  }

  @RTSerializer(GLRTask.Serializer.class)
  public static class GLRTask extends MRTask {
    GMatrix    _xx;
    GVector    _xy;
    VMap       _weights;
    Row2VecMap _rmap;

    public GLRTask(Row2VecMap rmap) {
      _rmap = rmap;
    }

    // private constructor to be used in deserialization of results
    private GLRTask(GMatrix xx, GVector xy) {
      _xx = xx;
      _xy = xy;
      _rmap = null;
    }

    @Override
    public void map(Key key) {
      assert key.home();
      Key aryKey = Key.make(ValueArray.getArrayKeyBytes(key));
      ValueArray ary = (ValueArray) DKV.get(aryKey);
      byte[] bits = DKV.get(key).get();
      int xlen = _rmap.xlen();
      GMatrix xx = new GMatrix(xlen,xlen);
      xx.setScale(0.0);
      _xy = new GVector(xlen);
      _xx = (GMatrix) xx.clone();
      _rmap.setRawData(ary, bits);
      System.out.println(_xx);
      for( Row r : _rmap ) {
        xx.mul(r.wx, r.x);
        _xx.add(xx);
        r.wx.scale(r.y);
        _xy.add(r.wx);
      }
      double nInv = 1 / (double) ary.num_rows();
      _xy.scale(nInv);
      xx.setScale(nInv);
      _xx.mul(xx);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GLRTask other = (GLRTask) drt;
      _xx.add(other._xx);
      _xy.add(other._xy);
    }


    public static class Serializer extends RemoteTaskSerializer<GLRTask> {

      @Override
      public int wire_len(GLRTask task) {
        if( task._xx == null && task._xy == null ) {
          // initial stage, data not computed yet, pass down the xs and y
          return 1 + task._rmap.wire_len();
        } else { // already computed data, hand them back
          return 1 + ((task._xx.getNumCol() * task._xx.getNumRow() + task._xy
              .getSize()) << 3);
        }
      }

      @Override
      public int write(GLRTask task, byte[] buf, int off) {
        if( task._xx == null && task._xy == null ) {
          buf[off++] = 1; // state 1
          off = task._rmap.write(buf, off);
        } else {
          buf[off++] = 2; // state 2
          assert task._xx.getNumCol() == task._xx.getNumRow();
          assert task._xy.getSize() == task._xx.getNumRow();
          UDP.set4(buf, off, task._xx.getNumRow());
          off += 4;
          int M = task._xx.getNumRow();
          int N = task._xx.getNumCol();
          for( int i = 0; i < M; ++i ) {
            for( int j = 0; j < N; ++j ) {
              UDP.set8d(buf, off, task._xx.getElement(i, j));
              off += 8;
            }
          }
          for( int i = 0; i < N; ++i ) {
            UDP.set8d(buf, off, task._xy.getElement(i));
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
          GMatrix xx = new GMatrix(xlen, xlen);
          for( int i = 0; i < xlen; ++i ) {
            for( int j = 0; j < xlen; ++j ) {
              xx.setElement(i, j, UDP.get8d(buf, off));
              off += 8;
            }
          }
          GVector xy = new GVector(xlen);
          for( int i = 0; i < xlen; ++i ) {
            xy.setElement(i, UDP.get8(buf, off));
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
