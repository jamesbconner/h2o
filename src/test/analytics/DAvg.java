package test.analytics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;

import water.DRemoteTask;
import water.Key;
import water.RemoteTask;
import water.UDP;
import water.UKV;
import water.Value;
import water.ValueArray;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.CSVString;
import water.csv.ValueCSVRecords;

/**
 * Simple class to test distributed parsing.
 * 
 * It runs of distributed csv parser and avg computation of data. Currently avg
 * is computed for all numerical columns.
 * 
 * To use, simply subclass it and pass correct values to constructor (see
 * PokerAvg.java)
 * 
 * @author tomas
 * 
 * @param <T>
 */
public abstract class DAvg<T> extends DRemoteTask {

  /**
   * 
   */
  private static final long serialVersionUID = 8096208064123681384L;

  CSVParserSetup _csvSetup;
  T _csvRecord;
  String[] _columns;

  CSVParserSetup _setup;
  double[] _values;
  int _N;
  boolean _isArray;

  Field[] _fields;

  public int nvalues() {
    return _values.length;
  }

  public double value(int i) {
    return _values[i];
  }

  void processColumn(int i, T r) throws IllegalArgumentException, IllegalAccessException {
    if (_isArray) {
      _values[i] += Array.getDouble(_csvRecord, i);
    } else {
      _values[i] += (double) _fields[i].getDouble(_csvRecord);
    }
  }

  public DAvg(T csvRecord, String[] columns, CSVParserSetup setup)
      throws NoSuchFieldException, SecurityException {
    _csvRecord = csvRecord;
    _columns = columns;
    _setup = setup;

    if (csvRecord.getClass().isArray()) {
      _values = new double[Array.getLength(csvRecord)];
      _isArray = true;
      if (csvRecord.getClass().getComponentType().equals(Integer.TYPE)) {

      } else if (csvRecord.getClass().getComponentType().equals(Double.TYPE)) {

      } else if (csvRecord.getClass().getComponentType().equals(Float.TYPE)) {

      } else if (CSVString.class
          .equals(csvRecord.getClass().getComponentType())) {
        throw new UnsupportedOperationException();
      } else if (String.class.equals(csvRecord.getClass().getComponentType())) {
        throw new UnsupportedOperationException();
      } else {
        throw new UnsupportedOperationException();
      }
    } else if (csvRecord.getClass().isInstance(Collection.class)) {
      throw new UnsupportedOperationException();
    } else {
      ArrayList<Field> fields = new ArrayList<Field>();
      for (String colName : columns) {
        if (colName != null) {
          Field f = csvRecord.getClass().getDeclaredField(colName);
          Type t = f.getGenericType();
          if (Integer.TYPE.equals(t)) {
            fields.add(f);
          } else if (Double.TYPE.equals(t)) {
            fields.add(f);
          } else if (Float.TYPE.equals(t)) {
            fields.add(f);
          }
        }
      }
      _fields = (Field[]) fields.toArray();
      _values = new double[_fields.length];
    }
  }

  public void rexec(Value v) {
    if (v.chunks() > 1) {
      Key[] keys = new Key[v.chunks()];
      for (int i = 0; i < v.chunks(); ++i) {
        keys[i] = v.chunk_get(i);
      }
      rexec(keys);
    }
  }

  @Override
  public void map(Key key) {
    int index = getChunkIndex(key);
    Key nextKey = getNextChunk(key);
    if (UKV.get(nextKey) == null)
      nextKey = null;
    try {
      ValueCSVRecords<T> records = new ValueCSVRecords<T>(key, nextKey, index,
          _csvRecord, _columns, _setup);
      for (T r : records) {
        ++_N;
        for (int i = 0; i < _values.length; ++i)
          processColumn(i, r);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error("unexpected exception");
    }
  }

  @Override
  public void reduce(RemoteTask drt) {
    DAvg<?> other = (DAvg<?>) drt;
    for (int i = 0; i < _values.length; ++i) {
      _values[i] += other._values[i];
    }
    _N += other._N;
  }

  @Override
  protected int wire_len() {
    return (_values != null) ? _values.length * Double.SIZE : 0;
  }

  @Override
  protected int write(byte[] buf, int off) {
    assert (buf.length - off) >= wire_len();
    int res = 0;
    for (int i = 0; i < _values.length; ++i) {
      long l = Double.doubleToLongBits(_values[i]);
      UDP.set8(buf, off + (i << 3), l);
      res += 8;
    }
    return res;
  }

  @Override
  protected void write(DataOutputStream dos) {
    for (int i = 0; i < _values.length; ++i) {
      try {
        dos.writeDouble(_values[i]);
      } catch (IOException e) {
        e.printStackTrace();
        throw new Error("unexpected exception " + e);
      }
    }
  }

  @Override
  protected void read(byte[] buf, int off) {
    for (int i = 0; i < _values.length; ++i) {
      long n = UDP.get8(buf, off + (i << 3));
      _values[i] = Double.longBitsToDouble(n);
    }
  }

  @Override
  protected void read(DataInputStream dis) {
    for (int i = 0; i < _values.length; ++i) {
      try {
        _values[i] = dis.readDouble();
      } catch (IOException e) {
        e.printStackTrace();
        throw new Error("unexpceted exception " + e);
      }
    }
  }

  // TODO temporary hack, need remote task to provide a way how to get current
  // key + next key for csv parsing
  // right now - I take it dircetly from the key assuming it is an arraylet
  // chunk and assuming the creation of arraylet keys does not change
  public static int getChunkIndex(Key k) {
    long n = UDP.get8(k._kb, 2);
    return (int) (n >> ValueArray.LOG_CHK);
  }

  public static Key getNextChunk(Key k) {
    byte[] arr = k._kb.clone();
    int index = (getChunkIndex(k) + 1);
    long n = ((long) index) << ValueArray.LOG_CHK;
    UDP.set8(arr, 2, n);
    return Key.make(arr);
  }

}
