package hex.rf;
import hex.rf.Tree.StatType;

import java.util.*;

import jsr166y.RecursiveAction;
import water.*;

/**
 * Distributed RandomForest
 * @author cliffc
 */
public class DRF extends water.DRemoteTask {
  // Cloud-wide data
  int _ntrees;          // Number of trees TOTAL, not per-node
  boolean _parallel;    // build trees in parallel or one by one
  int _depth;           // Tree-depth limiter
  int _stat;            // Use Gini(1) or Entropy(0) for splits
  int _classcol;        // Column being classified
  Key _arykey;          // The ValueArray being RF'd
  public Key _modelKey; // Where to jam the final trees
  public Key _treeskey; // Key of Tree-Keys built so-far
  int[] _ignores;       // Columns to ignore
  float _sample;        // Sampling rate
  short _bin_limit;     // Size of largest count-of-uniques in a column
  int _seed;            // Random # seed

  // Node-local data
  transient Data _validation;        // Data subset to validate with locally, or NULL
  transient RandomForest _rf;        // The local RandomForest

  public static class IllegalDataException extends Error {
    public IllegalDataException(String string) {
      super(string);
    }
  }

  private void validateInputData(ValueArray ary){
    if(ary.col_size(_classcol) < 0)throw new IllegalDataException("Floating point class column is not supported.");
    final int classes = (int)(ary.col_max(_classcol) - ary.col_min(_classcol))+1;
    // There is no point in running Rf when all the training data have the same class
    if( !(2 <= classes && classes <= 65534 ) )
      throw new IllegalDataException("Number of classes must be >= 2 and <= 65534, found " + classes);
  }

  public static DRF web_main( ValueArray ary, int ntrees, int depth, float sample, short binLimit, StatType stat, int seed, int classcol, int[] ignores, Key modelKey, boolean parallelTrees) {
    // Make a Task Key - a Key used by all nodes to report progress on RF
    DRF drf = new DRF();
    drf._parallel = parallelTrees;
    drf._ntrees = ntrees;
    drf._depth = depth;
    drf._stat = stat.ordinal();
    drf._arykey = ary._key;
    drf._classcol = classcol;
    drf._treeskey = Key.make("Trees of "+ary._key,(byte)1,Key.KEY_OF_KEYS);
    drf._seed = seed;
    drf._ignores = ignores;
    drf._modelKey = modelKey;
    assert sample <= 1.0f;
    drf._sample = sample;
    drf._bin_limit = binLimit;
    drf.validateInputData(ary);
    Utils.clearTimers();
    Utils.startTimer("maintimer");
    DKV.put(drf._treeskey, new Value(drf._treeskey, 4)); //4 bytes for the key-count, which is zero
    DKV.write_barrier();
    drf.fork(drf._arykey);
    return drf;
  }


  private static void binData(final DataAdapter dapt, final Key [] keys, final ValueArray ary, final int [] colIds, final int ncols){
    final int rowsize= ary.row_size();
    ArrayList<RecursiveAction> jobs = new ArrayList<RecursiveAction>();
    int start_row = 0;
    for(final Key k:keys) {
      if( !k.home() ) continue;
      final int rows = DKV.get(k)._max/rowsize;
      final int S = start_row;
      jobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          byte[] bits = DKV.get(k).get();
          ROWS: for(int j = 0; j < rows; ++j) {
            for(int c = 0; c < ncols; ++c)
              if( !ary.valid(bits,j,rowsize,colIds[c])) continue ROWS;
            for(int c = 0; c < ncols; ++c)
              dapt.addValueRaw((float)ary.datad(bits,j,rowsize,colIds[c]), j + S, colIds[c]);
          }
        }
      });
      start_row += rows;
    }
    invokeAll(jobs);
    // now do the binning
    jobs.clear();
    for(int c = 0; c < ncols; ++c){
      final int col = colIds[c];
      jobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          dapt.computeBins(col);
        }
      });
    }
    invokeAll(jobs);
  }

  public final  DataAdapter extractData(Key arykey, Key [] keys){
    final ValueArray ary = (ValueArray)DKV.get(arykey);
    final int rowsize = ary.row_size();

    // One pass over all chunks to compute max rows
    Utils.startTimer("maxrows");
    int num_rows = 0;
    int unique = -1;
    for( Key key : keys )
      if( key.home() ) {
        num_rows += DKV.get(key)._max/rowsize;
        if( unique == -1 )
          unique = ValueArray.getChunkIndex(key);
      }
    Utils.pln("[RF] Max/min done in "+ Utils.printTimer("maxrows"));

    Utils.startTimer("binning");
    // The data adapter...
    final DataAdapter dapt = new DataAdapter(ary, _classcol, _ignores, num_rows, unique, _seed, _bin_limit);
    // Now load the DataAdapter with all the rows on this Node
    final int ncolumns = ary.num_cols();

    ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();
    final Key [] ks = keys;

    // bin the columns, do at most 1/2 of the columns at once
    int colIds [] = new int[(ncolumns+1)>>1];
    int j = 0;
    int i = 0;
    for(; i < ncolumns && j < colIds.length; ++i)
      if( dapt.binColumn(i) ) colIds[j++] = i;
    binData(dapt, keys, ary, colIds, j);
    j = 0;
    for(; i < ncolumns; ++i)
      if( dapt.binColumn(i) ) colIds[j++] = i;
    if(j != 0) binData(dapt, keys, ary, colIds, j);
    Utils.pln("[RF] Binning done in " + Utils.printTimer("binning"));
    Utils.startTimer("inhale");

    // Build fast cutout for ignored columns
    final boolean icols[] = new boolean[ncolumns];
    for( int k : _ignores ) icols[k]=true;

    // now read the values
    int start_row = 0;
    for( final Key k : ks ) {
      final int S = start_row;
      if(!k.home())continue;
      final int rows = DKV.get(k)._max/rowsize;
      dataInhaleJobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          byte[] bits = DKV.get(k).get();
          ROWS: for(int j = 0; j < rows; ++j) {
            // Bail out of broken rows in not-ignored columns
            for(int c = 0; c < ncolumns; ++c)
              if( !icols[c] && !ary.valid(bits,j,rowsize,c)) continue ROWS;
            for( int c = 0; c < ncolumns; ++c) {
              if( !ary.valid(bits,j,rowsize,c)) {
              } else if( dapt.binColumn(c) ) {
                dapt.addValue((float)ary.datad(bits,j,rowsize,c), j + S, c);
              } else {
                long v = ary.data(bits,j,rowsize,c);
                v -= ary.col_min(c);
                dapt.addValue((short)v, S+j, c);
              }
            }
          }
        }
      });
      start_row += rows;
    }
    invokeAll(dataInhaleJobs);

    Utils.pln("[RF] Inhale done in " + Utils.printTimer("inhale"));

    return dapt;
  }
  // Local RF computation.
  public final void compute() {
    Utils.startTimer("extract");
    DataAdapter dapt = extractData(_arykey, _keys);
    Utils.pln("[RF] Data adapter built in " + Utils.printTimer("extract") );
    Data t = Data.make(dapt);
    _validation = t; // FIXME... this does not look right.

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
    _rf = new RandomForest(this, t, ntrees, _depth, 0.0, StatType.values()[_stat],_parallel);
    tryComplete();
  }

  public void reduce( DRemoteTask drt ) { }
}
