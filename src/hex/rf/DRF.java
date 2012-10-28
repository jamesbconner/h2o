package hex.rf;
import hex.rf.Tree.StatType;
import water.*;

import java.util.*;

import jsr166y.RecursiveAction;

/**
 * Distributed RandomForest
 * @author cliffc
 */
public class DRF extends water.DRemoteTask {
  // Cloud-wide data
  int _ntrees;          // Number of trees PER NODE
  int _depth;           // Tree-depth limiter
  int _stat;            // Use Gini(1) or Entropy(0) for splits
  int _classcol;        // Column being classified
  Key _arykey;          // The ValueArray being RF'd
  Key _modelKey;        // Where to jam the final trees
  public Key _treeskey; // Key of Tree-Keys built so-far
  int[] _ignores;

  // Node-local data
  transient Data _validation;        // Data subset to validate with locally, or NULL
  transient RandomForest _rf;        // The local RandomForest
  transient int _seed;

  public static class IllegalDataException extends Error {
    public IllegalDataException(String string) {
      super(string);
    }
  }

  private void validateInputData(ValueArray ary){
    final int classes = (int)(ary.col_max(_classcol) - ary.col_min(_classcol))+1;
    // There is no point in running Rf when all the training data have the same class
    if( !(2 <= classes && classes <= 65534 ) )
      throw new IllegalDataException("Number of classes must be >= 2 and <= 65534, found " + classes);
  }

  public static DRF web_main( ValueArray ary, int ntrees, int depth, double cutRate, StatType stat, int seed, int classcol, int[] ignores, Key modelKey) {
    // Make a Task Key - a Key used by all nodes to report progress on RF
    DRF drf = new DRF();
    drf._ntrees = ntrees;
    drf._depth = depth;
    drf._stat = stat.ordinal();
    drf._arykey = ary._key;
    drf._classcol = classcol;
    drf._treeskey = Key.make("Trees of "+ary._key,(byte)1,Key.KEY_OF_KEYS);
    drf._seed = seed;
    drf._ignores = ignores;
    drf._modelKey = modelKey;
    drf.validateInputData(ary);
    DKV.put(drf._treeskey, new Value(drf._treeskey, 4)); //4 bytes for the key-count, which is zero
    DKV.write_barrier();
    drf.fork(ary._key);
    return drf;
  }

  public final  DataAdapter extractData(Key arykey, Key [] keys){
    final ValueArray ary = (ValueArray)DKV.get(arykey);
    final int rowsize = ary.row_size();

    // One pass over all chunks to compute max rows
    int num_keys = 0;
    int num_rows = 0;
    int unique = -1;
    for( Key key : keys )
      if( key.home() ) {
        num_keys++;
        num_rows += DKV.get(key)._max/rowsize;
        if( unique == -1 )
          unique = ValueArray.getChunkIndex(key);
      }
    // The data adapter...
    final DataAdapter dapt = new DataAdapter(ary, _classcol, _ignores, num_rows, unique, _seed);
    // Now load the DataAdapter with all the rows on this Node
    RecursiveAction[] parts = new RecursiveAction[num_keys];
    num_keys = 0;
    num_rows = 0;
    for( final Key key : keys ) {
      if( key.home() ) {
        final int start_row = num_rows;
        parts[num_keys++] = new RecursiveAction() {
          @Override
          protected void compute() {
            float[] ds = new float[ary.num_cols()];
            byte[] bits = DKV.get(key).get();
            final int rows = bits.length/rowsize;
            ROW: for( int j = 0; j < rows; j++ ) { // For all rows in this chunk
              for( int k = 0; k < ds.length; k++ ) {
                // bad data means skip row
                if( !ary.valid(bits,j,rowsize,k) ) continue ROW;
                if(dapt.binColumn(k))
                  dapt.addValueRaw((float)ary.datad(bits,j,rowsize,k), start_row+j, k);
                else {
                  long v = ary.data(bits,j,rowsize,k);
                  v -= ary.col_min(k);
                  double d = ary.col_max(k);
                  if(d < 0)v += (long)d;
                  if(v < 0 || v >= 1024){
                    System.err.println("raw binn out of bounds: " + v + ", colsize = " + ary.col_size(k) + ", max=" + ary.col_mean(k) + ", min=" + ary.col_min(k) );
                  }
                  dapt.addValue((short)v, start_row+j, k);

                }
              }
            }
          }
        };
        num_rows += DKV.get(key)._max/rowsize;
      }
    }
    invokeAll(parts);             // first collect all the data row wise
    invokeAll(dapt.shrinkWrap()); // then shrink wrap the columns
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

    // Figure the number of trees to make locally, so the total hits ntrees.
    // Divide equally amongst all the nodes that actually have data.
    // First: compute how many nodes have data.
    ValueArray ary = (ValueArray)DKV.get(_arykey);
    final long num_chunks = ary.chunks();
    final int num_nodes = H2O.CLOUD.size();
    HashSet<H2ONode> nodes = new HashSet();
    for( long i=0; i<num_chunks; i++ ) {
      nodes.add(ary.chunk_get(i).home_node());
      if( nodes.size() == num_nodes ) // All of them?
        break;                        // Done
    }

    H2ONode[] array = nodes.toArray(new H2ONode[nodes.size()]);
    Arrays.sort(array);
    // Give each Node ntrees/#nodes worth of trees.  Round down for later nodes,
    // and round up for earlier nodes.
    int ntrees = _ntrees/nodes.size();
    if( Arrays.binarySearch(array, H2O.SELF) < _ntrees - ntrees*nodes.size() )
      ++ntrees;

    // Make a single RandomForest to that does all the tree-construction work.
    Utils.pln("[RF] Building "+ntrees+" trees");
    _rf = new RandomForest(this, t, ntrees, _depth, 0.0, StatType.values()[_stat]);
    tryComplete();
  }

  static boolean sample;
  static boolean forceNoSample = false;

  public void reduce( DRemoteTask drt ) { }
}
