package hexlytics.rf;

import java.io.*;
import water.*;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

/**
 * @author cliffc
 */
@RTSerializer(DRF.Serializer.class)
public class DRF extends water.DRemoteTask {
  int _ntrees;                  // Number of trees PER NODE
  int _depth;                   // Tree-depth limiter
  boolean _useGini;             // Use Gini (true) vs Entropy (false) for splits
  Key _arykey;                  // The ValueArray being RF'd
  Key _treeskey;                // Key of Tree-Keys built so-far

  public static class Serializer extends RemoteTaskSerializer<DRF> {
    @Override public int wire_len(DRF t) { return 4+4+1+t._arykey.wire_len(); }
    @Override public int write( DRF t, byte[] buf, int off ) {
      off += UDP.set4(buf,off,t._ntrees);
      off += UDP.set4(buf,off,t._depth);
      buf[off++] = (byte)(t._useGini ? 1:0);
      off = t.  _arykey.write(buf,off);
      off = t._treeskey.write(buf,off);
      return off;
    }
    @Override public DRF read( byte[] buf, int off ) {
      DRF t = new DRF();
      t._ntrees= UDP.get4(buf,(off+=4)-4);
      t._depth = UDP.get4(buf,(off+=4)-4);
      t._useGini = buf[off++]==1 ? true : false;
      t.  _arykey = Key.read(buf,off);  off += t.  _arykey.wire_len();
      t._treeskey = Key.read(buf,off);  off += t._treeskey.wire_len();
      return t;
    }
    @Override public void write( DRF t, DataOutputStream dos ) { throw new Error("do not call"); }
    @Override public DRF  read (        DataInputStream  dis ) { throw new Error("do not call"); }
  }

  public static Key web_main( ValueArray ary, int ntrees, int depth, double cutRate, boolean useGini) {
    // Make a Task Key - a Key used by all nodes to report progress on RF
    DRF drf = new DRF();
    drf._ntrees = ntrees;
    drf._depth = depth;
    drf._useGini = useGini;
    drf._arykey = ary._key;
    drf._treeskey = Key.make("Trees of "+ary._key,(byte)1,Key.KEY_OF_KEYS);
    DKV.put(drf._treeskey, new Value(drf._treeskey,4/*4 bytes for the key-count, which is zero*/));
    drf.fork(ary._key);
    return drf._treeskey;
  }

  // Local RF computation.
  public final void compute() {
    ValueArray ary = (ValueArray)DKV.get(_arykey);
    final int rowsize = ary.row_size();
    final int num_cols = ary.num_cols();
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
    DataAdapter dapt =  new DataAdapter(ary._key.toString(), names, names[num_cols-1], num_rows, unique);
    double[] ds = new double[num_cols];
    // Now load the DataAdapter with all the rows
    for( Key key : _keys ) {
      if( key.home() ) {
        byte[] bits = DKV.get(key).get();
        final int rows = bits.length/rowsize;
        for( int j=0; j<rows; j++ ) { // For all rows in this chunk
          for( int k=0; k<num_cols; k++ )
            ds[k] = ary.datad(bits,j,rowsize,k);
          dapt.addRow(ds);
        }
      }
    }
    dapt.shrinkWrap();
    System.out.println("Invoking RF ntrees="+_ntrees+" depth="+_depth+" gini="+_useGini);
    RandomForest rf = new RandomForest(this,dapt, .666, _ntrees, _depth, -1, _useGini ? Tree.StatType.gini : Tree.StatType.numeric);
    tryComplete();
  }

  // Reducing RF's from all over in a log-tree roll-up
  public void reduce( DRemoteTask drt ) {
    DRF drf = (DRF)drt;
  }
}
