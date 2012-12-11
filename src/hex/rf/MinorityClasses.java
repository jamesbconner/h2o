package hex.rf;

import init.H2OSerializable;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import water.*;

/**
 * Contains methods for extracting minority classes out of value array and redistributing them to all nodes.
 *
 * Class is considered to be minor if it's frequency relative to the majority class is below given threshold.
 * Setting the threshold to 1.0 will cause all classes with fewer occurences than the majority class to be redistributed everywhere.
 * Setting it to 0 will cause none of the classes to be redistributed.
 *
 * Major class(es) is(are) always left untouched.
 *
 * Classes are extracted into new Values and their keys are accumulated. Redistribution step takes those keys as the input
 * and redistributes them across all nodes.
 *
 * @author tomasnykodym
 *
 */
public class MinorityClasses {

  //class c is considered to be minority class (ie all rows with this class will be replicated across ALL nodes)
  // iff H(c)/H(m) <= MINORITY_RATIO_THRESHOLD && H(c)/Nodes <= MINORITY_ABS_THRESHOLD,
  //     where H(c) is histogram value for class c, H(m) is histogram value for majority class, Nodes is number of nodes in the cloud
  static final double MINORITY_RATIO_THRESHOLD = 0.1;
  static final double MINORITY_ABS_THRESHOLD = 1000000;

  //class c is considered to be unbalanced (unevenly distributed) iff abs((Hn(c) - H(c)/Nodes)/H(c) > IMBALANCED_THRESHOLD for some node n
  static final double IMBALANCED_THRESHOLD = 0.1;

  public static int [][] histogram(ValueArray data, int classIdx){
    HistogramTask tsk = new HistogramTask();
    tsk._n = (int)(data.col_max(classIdx) - data.col_min(classIdx))+1;
    tsk._classIdx = classIdx;
    tsk._aryKey = data._key;
    tsk.invoke(data._key);
    return tsk._histogram;
  }

  public static int [][] histogram(ValueArray data, Key[] keys, int classIdx){
    HistogramTask tsk = new HistogramTask();
    tsk._n = (int)(data.col_max(classIdx) - data.col_min(classIdx))+1;
    tsk._n = (int)data.col_max(classIdx)+1;
    tsk._classIdx = classIdx;
    tsk._aryKey = data._key;
    tsk.invoke(keys);
    return tsk._histogram;
  }

  static double [] classRatios(int [] globalHistogram){
    int max = globalHistogram[0];
    for(int i = 1; i < globalHistogram.length; ++i)
      if(globalHistogram[i] > max)max = globalHistogram[i];
    double [] res = new double [globalHistogram.length];
    double inv = 1.0/max;
    for(int i = 0; i < globalHistogram.length; ++i)
      res[i] = globalHistogram[i]*inv;
    return res;
  }

  static double [][] classNodeDistribution(int [][] histogram){
    long [] sum = new long[histogram[0].length];
    for(int c = 0; c < histogram[0].length; ++c){
      for(int n = 0; n < histogram.length; ++n)
        sum[c] += histogram[n][c];
    }
    double [][] res = new double[histogram.length][histogram[0].length];
    for(int c = 0; c < histogram[0].length; ++c){
      double div = 1.0/sum[c];
      for(int n = 0; n < histogram.length; ++n)
        res[n][c] = histogram[n][c]*div;
    }
    return res;
  }


  public static long countMajorityClassRows(Key aryKey, Key [] keys, int [] minorities, int classIdx){
    CountMClassRowsTask tsk = new CountMClassRowsTask();
    tsk._aryKey = aryKey;
    tsk._minorities = minorities;
    tsk._classIdx = classIdx;
    tsk.invoke(keys);
    return tsk._res;
  }

  public static class CountMClassRowsTask extends MRTask {
    Key _aryKey;
    int [] _minorities;
    int _classIdx;
    long _res;
    @Override
    public void map(Key key) {
      ValueArray ary = ((_aryKey != null)?(ValueArray)DKV.get(_aryKey):null);
      byte [] bits = DKV.get(key).get();
      int row_size = ary.row_size();
      int off = ary.col_off(_classIdx);
      int size = ary.col_size(_classIdx);
      int scale = ary.col_scale(_classIdx);
      int base = ary.col_base(_classIdx);
      int rows = bits.length/ary.row_size();
      for(int i = 0; i < rows; ++i)
        if(Arrays.binarySearch(_minorities, (int)ary.data(bits, i, row_size, off, size, base, scale, _classIdx)) < 0)
          ++_res;
    }

    @Override
    public void reduce(DRemoteTask drt) {
      _res += ((CountMClassRowsTask)drt)._res;
    }

  };

  public static class UnbalancedClass implements H2OSerializable {
    public int _c;
    public Key [] _chunks;
    public int _rows;
    public UnbalancedClass(int c, Key [] chunks, int rows){
      _c = c;
      _chunks = chunks;
      _rows = rows;
    }
  }

  public static UnbalancedClass[] extractUnbalancedClasses(ValueArray data, int classIdx, int [] classIds){
    ClassExtractTask tsk = new ClassExtractTask();
    tsk._imbalancedClasses = classIds;
    tsk._classColumnIdx = classIdx;
    tsk._rowsize = data.row_size();
    tsk.invoke(data._key);
    // deal with the remainders
    for(int i = 0; i < tsk._offsets.length; ++i)
      if(tsk._offsets[i] > 0)
        tsk.storeValue(i, tsk._bufs[i]);
    UnbalancedClass [] res = new UnbalancedClass[tsk._imbalancedClasses.length];
    for(int i = 0; i < tsk._imbalancedClasses.length; ++i)
      res[i] = new UnbalancedClass(tsk._imbalancedClasses[i], tsk._createdKeys[i], tsk._histogram[i]);
    return res;
  }

  public static int [] globalHistogram(int [][] histogram){
    int [] h = new int[histogram[0].length];
    for(int i = 0; i < histogram.length; ++i)
      for(int j = 0; j < histogram[i].length; ++j)
        h[j] += histogram[i][j];
    return h;
  }

  public static class HistogramTask extends MRTask {
    int _n;
    int [][] _histogram;
    int _classIdx;
    Key _aryKey;

    public int [] histogram(int nodeIdx){
      return _histogram[nodeIdx];
    }


    @Override
    public void map(Key key) {
      ValueArray ary = ((_aryKey != null)?(ValueArray)DKV.get(_aryKey):null);
      _histogram = new int[H2O.CLOUD.size()][_n];
      byte [] bits = DKV.get(key).get();
      int row_size = ary.row_size();
      int off = ary.col_off(_classIdx);
      int size = ary.col_size(_classIdx);
      int scale = ary.col_scale(_classIdx);
      int base = ary.col_base(_classIdx);
      int rows = bits.length/ary.row_size();
      int min = (int)ary.col_min(_classIdx);
      for(int i = 0; i < rows; ++i)
        ++_histogram[H2O.SELF.index()][(int)ary.data(bits, i, row_size, off, size, base, scale, _classIdx) - min];
    }

    @Override
    public void reduce(DRemoteTask drt) {
      HistogramTask other = (HistogramTask)drt;
      if(_histogram == null)
        _histogram = other._histogram;
      else
        for(int i = 0; i < H2O.CLOUD.size(); ++i)
          for(int j = 0; j < _histogram[0].length; ++j)
            _histogram[i][j] += other._histogram[i][j];
    }
  }

  public static class ReplicationTask extends MRTask {
    Key [] _createdKeys;
    @Override
    public void map(Key key) {
      _createdKeys = new Key[H2O.CLOUD._memary.length];
      byte [] bits = DKV.get(key).get();
      for(int i = 0; i < _createdKeys.length; ++i){
        if(H2O.SELF.index() == i)
          _createdKeys[i] = key;
        else {
          _createdKeys[i] = Key.make(Key.make()._kb, (byte)1, Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[i]);
          DKV.put(_createdKeys[i], new Value(_createdKeys[i],bits));
        }
      }
    }

    @Override
    public void reduce(DRemoteTask drt) {
      ReplicationTask other = (ReplicationTask)drt;
      if(_createdKeys == null)_createdKeys = other._createdKeys;
      else {
        int n = _createdKeys.length;
        _createdKeys = Arrays.copyOf(_createdKeys, _createdKeys.length + other._createdKeys.length);
        System.arraycopy(other._createdKeys, 0, _createdKeys, n, other._createdKeys.length);
      }
    }
  }

  public static class ClassExtractTask extends MRTask {
    int _classColumnIdx;
    int _rowsize;
    int [] _imbalancedClasses; // classes we're interested in extacting out
    int [] _histogram;
    byte [][] _bufs; // buffer for each minority class
    int [] _offsets;
    Key [][] _createdKeys;

    void storeValue(int i, byte [] bits){
      Key k = Key.make();//Key.make()._kb,(byte)1,Key.DFJ_INTERNAL_USER,H2O.SELF);
      _createdKeys[i] = (_createdKeys[i] == null)?new Key[1]:Arrays.copyOf(_createdKeys[i], _createdKeys[i].length + 1);
      _createdKeys[i][_createdKeys[i].length-1] = k;
      Value v = new Value(k, bits);
      DKV.put(k, v);
    }

    @Override
    public int wire_len(){
      return 4 + 4
          + UDP.wire_len(_imbalancedClasses)
          + UDP.wire_len(_bufs)
          + UDP.wire_len(_offsets)
          + SerializationUtils.wire_len(_createdKeys)
          + UDP.wire_len(_histogram);
    }

    @Override
    public void read(DataInputStream dis) throws IOException{
      _classColumnIdx = dis.readInt();
      _rowsize = dis.readInt();
      _imbalancedClasses = TCPReceiverThread.readIntAry(dis);
      _bufs = TCPReceiverThread.readByteByteAry(dis);
      _offsets = TCPReceiverThread.readIntAry(dis);
      _createdKeys = SerializationUtils.read2DKeyArray(dis);
      _histogram = TCPReceiverThread.readIntAry(dis);
    }

    @Override
    public void read(Stream s){
      _classColumnIdx = s.get4();
      _rowsize = s.get4();
      _imbalancedClasses = s.getAry4();
      _bufs = s.getAry11();
      _offsets = s.getAry4();
      _createdKeys = SerializationUtils.read2DKeyArray(s);
      _histogram = s.getAry4();
    }

    @Override
    public void write(DataOutputStream dos) throws IOException{
      dos.writeInt(_classColumnIdx);
      dos.writeInt(_rowsize);
      TCPReceiverThread.writeAry(dos, _imbalancedClasses);
      TCPReceiverThread.writeAry(dos,_bufs);
      TCPReceiverThread.writeAry(dos,_offsets);
      SerializationUtils.write(_createdKeys,dos);
      TCPReceiverThread.writeAry(dos,_histogram);
    }

    @Override
    public void write(Stream s){
      s.set4(_classColumnIdx);
      s.set4(_rowsize);
      s.setAry4(_imbalancedClasses);
      s.setAry11(_bufs);
      s.setAry4(_offsets);
      SerializationUtils.write(_createdKeys, s);
      s.setAry4(_histogram);
    }


    @Override
    public void map(Key key) {
      Key aryKey = Key.make(ValueArray.getArrayKeyBytes(key));
      ValueArray ary = (ValueArray)DKV.get(aryKey);
      _offsets = new int[_imbalancedClasses.length];
      _histogram = new int[_imbalancedClasses.length];
      _createdKeys = new Key[_imbalancedClasses.length][];
      _bufs = new byte[_imbalancedClasses.length][];
      for(int i = 0; i < _bufs.length; ++i)
        _bufs[i] = new byte[1 << (ValueArray.LOG_CHK - 5)];
      byte [] bits = DKV.get(key).get();
      int _rowsize = ary.row_size();
      int off = ary.col_off(_classColumnIdx);
      int size = ary.col_size(_classColumnIdx);
      int scale = ary.col_scale(_classColumnIdx);
      int base = ary.col_base(_classColumnIdx);
      int rows = bits.length/ary.row_size();
      int chunksize = rows*_rowsize;
      for(int i = 0; i < rows; ++i) {
        int idx = Arrays.binarySearch(_imbalancedClasses, (int)ary.data(bits, i, _rowsize, off, size, base, scale, _classColumnIdx));
        if(idx >= 0) {
          ++_histogram[idx];
          // if we run out of space ,go to the full chunk size
          if((_offsets[idx] + _rowsize) >= _bufs[idx].length)
            _bufs[idx] = Arrays.copyOf(_bufs[idx], chunksize);
          if((_offsets[idx] + _rowsize) >= chunksize)
            System.out.println("haha");
          System.arraycopy(bits,i*_rowsize, _bufs[idx], _offsets[idx], _rowsize);
          _offsets[idx] += _rowsize;
        }
      }
      // now check if any of the chunks is ready to be written
      // (unlikely unless dataset is sorted by class)
      for(int i = 0; i < _offsets.length; ++i){
        if(_offsets[i] == chunksize){
          storeValue(i, _bufs[i]);
          _offsets[i] = 0;
        } else
          assert _offsets[i] < chunksize;
      }
    }

    @Override
    public void reduce(DRemoteTask drt) {
      ClassExtractTask other = (ClassExtractTask)drt;
      if(_bufs == null) {
        assert _createdKeys == null;
        assert _offsets == null;
        _bufs = other._bufs;
        _createdKeys = other._createdKeys;
        _offsets = other._offsets;
        _histogram = other._histogram;
      } else {
        if(_histogram == null || other._histogram == null)
          System.out.println("haha");
        for(int i = 0; i < _histogram.length; ++i)
          _histogram[i] += other._histogram[i];
        for(int i = 0; i < _bufs.length; ++i){
          if(_offsets[i] == 0) {
            _bufs[i] = other._bufs[i];
            _offsets[i] = other._offsets[i];
          } else if (other._offsets[i] > 0){
            if(_offsets[i] + other._offsets[i] < ValueArray.chunk_size()){
              _bufs[i] = Arrays.copyOf(_bufs[i], _offsets[i] + other._offsets[i]);
              System.arraycopy(other._bufs[i], 0, _bufs[i], _offsets[i], other._offsets[i]);
              _offsets[i] += other._offsets[i];
            } else {
              // create a new value
              int rpc = (int)ValueArray.chunk_size() / _rowsize;
              int chunksize = rpc*_rowsize;
              byte [] bits = MemoryManager.arrayCopyOf(_bufs[i], chunksize);
              storeValue(i, bits);
              assert chunksize > _offsets[i];
              System.arraycopy(other._bufs[i], 0, bits, _offsets[i], chunksize - _offsets[i]);
              _bufs[i] = Arrays.copyOfRange(other._bufs[i], chunksize - _offsets[i], other._offsets[i]);
              _offsets[i] = _bufs[i].length;
            }
          }
        }
        // now join the keys
        for(int i = 0; i < _createdKeys.length; ++i){
          if(_createdKeys[i] == null) _createdKeys[i] = other._createdKeys[i];
          else if(other._createdKeys[i] != null){
            int off = _createdKeys[i].length;
            _createdKeys[i] = Arrays.copyOf(_createdKeys[i], _createdKeys[i].length + other._createdKeys[i].length);
            System.arraycopy(other._createdKeys[i], 0, _createdKeys[i], off, other._createdKeys[i].length);
          }
        }
      }
    }
  }
}