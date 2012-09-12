package hexlytics.rf;

import hexlytics.rf.Tree.StatType;

import java.io.*;
import water.*;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

/**
 * Distributed RandomForest
 * @author cliffc
 */
@RTSerializer(DRF.Serializer.class)
public class DRF extends water.DRemoteTask {

  int _ntrees;                  // Number of trees PER NODE
  int _depth;                   // Tree-depth limiter
  StatType _stat;               // Use Gini or Entropy for splits
  Key _arykey;                  // The ValueArray being RF'd
  public Key _treeskey;         // Key of Tree-Keys built so-far
  Data _validation;             // Data subset to validate with locally, or NULL
  boolean _singlethreaded;      // Disable parallel execution
  RandomForest _rf;             // The local RandomForest

  public static class Serializer extends RemoteTaskSerializer<DRF> {
    @Override public int wire_len(DRF t) { return 4+4+1+t._arykey.wire_len(); }
    @Override public int write( DRF t, byte[] buf, int off ) {
      off += UDP.set4(buf,off,t._ntrees);
      off += UDP.set4(buf,off,t._depth);
      buf[off++] = (byte)(t._stat.ordinal());
      off = t.  _arykey.write(buf,off);
      off = t._treeskey.write(buf,off);
      return off;
    }
    @Override public DRF read( byte[] buf, int off ) {
      DRF t = new DRF();
      t._ntrees= UDP.get4(buf,(off+=4)-4);
      t._depth = UDP.get4(buf,(off+=4)-4);
      t._stat  = StatType.values()[buf[off++]];
      t.  _arykey = Key.read(buf,off);  off += t.  _arykey.wire_len();
      t._treeskey = Key.read(buf,off);  off += t._treeskey.wire_len();
      return t;
    }
    @Override public void write( DRF t, DataOutputStream dos ) { throw new Error("do not call"); }
    @Override public DRF  read (        DataInputStream  dis ) { throw new Error("do not call"); }
  }

  public static DRF web_main( ValueArray ary, int ntrees, int depth, double cutRate, StatType stat, boolean singlethreaded) {
    // Make a Task Key - a Key used by all nodes to report progress on RF
    DRF drf = new DRF();
    drf._ntrees = ntrees;
    drf._depth = depth;
    drf._stat = stat;
    drf._arykey = ary._key;
    drf._treeskey = Key.make("Trees of "+ary._key,(byte)1,Key.KEY_OF_KEYS);
    drf._singlethreaded = singlethreaded;
    Tree.THREADED = !singlethreaded;
    DKV.put(drf._treeskey, new Value(drf._treeskey, 4)); //4 bytes for the key-count, which is zero
    DKV.write_barrier();
    if( singlethreaded ) drf.invoke(ary._key);
    else                 drf.fork  (ary._key);
    return drf;
  }
  // Local RF computation.
  public final void compute() {
    ValueArray ary = (ValueArray)DKV.get(_arykey);
    final int rowsize = ary.row_size();
    final int num_cols = ary.num_cols();
    final int classes = (int)(ary.col_max(num_cols-1) - ary.col_min(num_cols-1))+1;
    assert 0 <= classes && classes < 255;
    String[] names = ary.col_names();

    // One pass over all chunks to compute max rows
    int num_rows = 0;
    int unique = -1;
    for( Key key : _keys )
      if( key.home() ) {
        // An NPE here means the cloud is changing...
        num_rows += DKV.get(key)._max/rowsize;
        if( unique == -1 )
          unique = ValueArray.getChunkIndex(key);
      }
    // The data adapter...
    DataAdapter dapt =  new DataAdapter(ary._key.toString(), names, names[num_cols-1], num_rows, unique, classes);
    float[] ds = new float[num_cols];
    // Now load the DataAdapter with all the rows on this Node
    for( Key key : _keys ) {
      if( key.home() ) {
        byte[] bits = DKV.get(key).get();
        final int rows = bits.length/rowsize;
        for( int j=0; j<rows; j++ ) { // For all rows in this chunk
          ds[num_cols-1] = Float.NaN; // Row-has-invalid-data flag
          for( int k=0; k<num_cols; k++ ) {
            if( !ary.valid(bits,j,rowsize,k) ) break; // oops, bad data on row
            ds[k] = (float)ary.datad(bits,j,rowsize,k);
          }
          if( !Float.isNaN(ds[num_cols-1]) ) // Insert only good rows
            dapt.addRow(ds);                 // Insert row
        }
      }
    }
    dapt.shrinkWrap();

    // If we have too little data to validate distributed, then
    // split the data now with sampling and train on one set & validate on the other.
    sample = sample || _keys.length < 2; // Sample if we only have 1 key, hence no distribution
    Data d = Data.make(dapt);
    Data t = sample ? d.sampleWithReplacement(.666) : d;
    _validation = sample ? t.complement() : null;
    // Make a single RandomForest to that does all the tree-construction work.
    _rf = new RandomForest(this, t, _ntrees, _depth, 0.0, _stat, _singlethreaded);
    tryComplete();
  }

  static boolean sample;

  public void reduce( DRemoteTask drt ) { }
}
