package hex.rf;
import hex.rf.Tree.StatType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
  int _numrows;         // Used to replay sampling
  short _bin_limit;     // Size of largest count-of-uniques in a column
  int _seed;            // Random # seed
  double[] _classWt;    // Class weights

  int _features;

  // Node-local data
  transient Data _validation;        // Data subset to validate with locally, or NULL
  transient RandomForest _rf;        // The local RandomForest
  transient Timer _t_main;     // Main timer

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

  public static DRF web_main( ValueArray ary, int ntrees, int depth, float sample, short binLimit, StatType stat, int seed, int classcol, int[] ignores, Key modelKey, boolean parallelTrees, double[] classWt, int features) {
    // Make a Task Key - a Key used by all nodes to report progress on RF
    DRF drf = new DRF();
    assert features==-1 || ((features>0) && (features<ary.num_cols()-1));
    drf._features = features;
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
    assert 0.0f <= sample && sample <= 1.0f;
    drf._sample = sample;
    drf._bin_limit = binLimit;
    drf._classWt = classWt;
    drf.validateInputData(ary);
    drf._t_main = new Timer();
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

  public final  DataAdapter extractData(Key arykey, final Key [] keys) {
    final ValueArray ary = (ValueArray)DKV.get(arykey);
    final int rowsize = ary.row_size();
    _numrows = DKV.get(keys[0])._max/rowsize; // Rows-per-chunk

    // One pass over all chunks to compute max rows
    Timer t_max = new Timer();
    int num_rows = 0;
    int unique = -1;
    for( Key key : keys )
      if( key.home() ) {
        num_rows += DKV.get(key)._max/rowsize;
        if( unique == -1 )
          unique = ValueArray.getChunkIndex(key);
      }
    Utils.pln("[RF] Max/min done in "+ t_max);

    Timer t_bin = new Timer();
    // The data adapter...
    final DataAdapter dapt = new DataAdapter(ary, _classcol, _ignores, num_rows, unique, _seed, _bin_limit, _classWt);
    // Now load the DataAdapter with all the rows on this Node
    final int ncolumns = ary.num_cols();

    ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();

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
    Utils.pln("[RF] Binning done in " + t_bin);

    Timer t_inhale = new Timer();
    // Build fast cutout for ignored columns
    final boolean icols[] = new boolean[ncolumns];
    for( int k : _ignores ) icols[k]=true;

    // now read the values
    int start_row = 0;
    for( final Key k : keys ) {
      final int S = start_row;
      if(!k.home())continue;
      final int rows = DKV.get(k)._max/rowsize;
      dataInhaleJobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          byte[] bits = DKV.get(k).get();
          ROWS: for(int j = 0; j < rows; ++j) {
            for(int c = 0; c < ncolumns; ++c) // Bail out of broken rows in not-ignored columns
              if( !icols[c] && !ary.valid(bits,j,rowsize,c)) {
                dapt.setBad(S+j);
                continue ROWS;
              }
            for( int c = 0; c < ncolumns; ++c)
              if( icols[c] )
                dapt.addValue((short)0,S+j,c);
              else if( dapt.binColumn(c) ) {
                dapt.addValue((float)ary.datad(bits,j,rowsize,c), S+j, c);
              } else {
                long v = ary.data(bits,j,rowsize,c);
                v -= ary.col_min(c);
                dapt.addValue((short)v, S+j, c);
              }
          }
        }
      });
      start_row += rows;
    }
    invokeAll(dataInhaleJobs);
    Utils.pln("[RF] Inhale done in " + t_inhale);
    return dapt;
  }
  // Local RF computation.
  public final void compute() {
    Timer t_extract = new Timer();
    DataAdapter dapt = extractData(_arykey, _keys);
    Utils.pln("[RF] Data adapter built in " + t_extract );
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
    _rf = new RandomForest(this, t, ntrees, _depth, 0.0, StatType.values()[_stat],_parallel,_features,_ignores);
    tryComplete();
  }

  public void reduce( DRemoteTask drt ) { }
}
