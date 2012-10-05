package hex.rf;
import hex.rf.Tree.StatType;
import water.*;

/**
 * Distributed RandomForest
 * @author cliffc
 */
public class DRF extends water.DRemoteTask {
  // Cloud-wide data
  int _ntrees;          // Number of trees PER NODE
  int _depth;           // Tree-depth limiter
  int _stat;            // Use Gini(1) or Entropy(0) for splits
  Key _arykey;          // The ValueArray being RF'd
  public Key _treeskey; // Key of Tree-Keys built so-far

  // Node-local data
  transient Data _validation;        // Data subset to validate with locally, or NULL
  transient boolean _singlethreaded; // Disable parallel execution
  transient RandomForest _rf;        // The local RandomForest
  transient int _seed;

  public static class IllegalDataException extends Error {
    public IllegalDataException(String string) {
      super(string);
    }
  }

  private static void validateInputData(ValueArray ary){
    final int num_cols = ary.num_cols();
    final int classes = (int)(ary.col_max(num_cols-1) - ary.col_min(num_cols-1))+1;
    // There is no point in running Rf when all the training data have the same
    // class, however it is currently failing the test/build
    if( !(2 <= classes && classes < 255 ) )
      throw new IllegalDataException("Number of classes must be between 2 and 254, found " + classes);
  }

  public static DRF web_main( ValueArray ary, int ntrees, int depth, double cutRate, StatType stat, int seed, boolean singlethreaded) {
    validateInputData(ary);
    // Make a Task Key - a Key used by all nodes to report progress on RF
    DRF drf = new DRF();
    drf._ntrees = ntrees;
    drf._depth = depth;
    drf._stat = stat.ordinal();
    drf._arykey = ary._key;
    drf._treeskey = Key.make("Trees of "+ary._key,(byte)1,Key.KEY_OF_KEYS);
    drf._singlethreaded = singlethreaded;
    drf._seed = seed;
    Tree.THREADED = !singlethreaded;
    DKV.put(drf._treeskey, new Value(drf._treeskey, 4)); //4 bytes for the key-count, which is zero
    DKV.write_barrier();
    if( singlethreaded ) drf.invoke(ary._key);
    else                 drf.fork  (ary._key);
    return drf;
  }

  public final  DataAdapter extractData(Key arykey, Key [] keys){
    ValueArray ary = (ValueArray)DKV.get(arykey);
    final int rowsize = ary.row_size();
    final int num_cols = ary.num_cols();
    final int classes = (int)(ary.col_max(num_cols-1) - ary.col_min(num_cols-1))+1;
    assert 0 <= classes && classes < 255;
    String[] names = ary.col_names();

    // One pass over all chunks to compute max rows
    int num_rows = 0;
    int unique = -1;
    for( Key key : keys )
      if( key.home() ) {
        // An NPE here means the cloud is changing...
        num_rows += DKV.get(key)._max/rowsize;
        if( unique == -1 )
          unique = ValueArray.getChunkIndex(key);
      }
    // The data adapter...
    DataAdapter dapt =  new DataAdapter(ary._key.toString(), names, names[num_cols-1], num_rows, unique, _seed, classes);
    float[] ds = new float[num_cols];
    // Now load the DataAdapter with all the rows on this Node
    for( Key key : keys ) {
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
    return dapt;
  }
  // Local RF computation.
  public final void compute() {
    DataAdapter dapt = extractData(_arykey, _keys);
    Utils.pln("[RF] Data adapter built");
    // If we have too little data to validate distributed, then
    // split the data now with sampling and train on one set & validate on the other.
    sample = (!forceNoSample) && sample || _keys.length < 2; // Sample if we only have 1 key, hence no distribution
    Data d = Data.make(dapt);
    short[] complement = sample ? new short[d.rows()] : null;
    Data t = sample ? d.sampleWithReplacement(.666, complement) : d;
    _validation = sample ? t.complement(d, complement) : null;
    // Make a single RandomForest to that does all the tree-construction work.
    Utils.pln("[RF] Building trees");
    _rf = new RandomForest(this, t, _ntrees, _depth, 0.0, StatType.values()[_stat], _singlethreaded);
    tryComplete();
  }

  static boolean sample;
  static boolean forceNoSample = false;

  public void reduce( DRemoteTask drt ) { }
}
