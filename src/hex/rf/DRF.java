package hex.rf;
import hex.rf.MinorityClasses.UnbalancedClass;
import hex.rf.Tree.StatType;

import java.util.*;

import jsr166y.RecursiveAction;
import water.*;
import water.Timer;
import water.ValueArray.Column;

/** Distributed RandomForest */
public class DRF extends water.DRemoteTask {
  boolean _useStratifySampling;
  int [][] _histogram;

  // OPTIONS FOR RF
  /** Total number of trees  (default: 10) */
  int _ntrees;
  /** If true, build trees in parallel (default: true) */
  boolean _parallel;
  /** Maximum depth for trees (default MaxInt) */
  int _depth;
  /** Split statistic (1=Gini, 0=Entropy; default 1) */
  int _stat;
  /** Feature holding the classifier  (default: #features-1) */
  int _classcol;
  /** Features to ignore (default: none) */
  int[] _ignores;
  /** Proportion of observations to use for building each individual tree (default: .67)*/
  float _sample;
  /** Used to replay sampling */
  int _numrows;
  /** Limit of the cardinality of a feature */
  short _binLimit;
  /** Pseudo random seed */
  long _seed;
  /** Weights of the different features (default: 1/features) */
  double[] _classWt;

  // INTERNAL DATA
  /** Key for the data being classified */
  public Key _arykey;
  UnbalancedClass [] _uClasses;
  int [] _strata;
  /** Key for the model being buildt */
  public Key _modelKey;
  /** Key for the trees built so far*/
  public Key _treeskey;

  /* Number of features which are tried at each split */
  public int _features;

  // Node-local data
  transient Data _validation;        // Data subset to validate with locally, or NULL
  transient RandomForest _rf;        // The local RandomForest
  transient public Timer _t_main;    // Main timer

  int [][] _nHist;
  int [] _gHist;
  public static class IllegalDataException extends Error {
    public IllegalDataException(String string) { super(string); }
  }

  private void validateInputData(ValueArray ary){
    Column c = ary._cols[_classcol];
    if(c._size < 0)
      throw new IllegalDataException("Floating point class column is not supported.");
    final int classes = (int)(c._max - c._min)+1;
    // There is no point in running Rf when all the training data have the same class
    if( !(2 <= classes && classes <= 65534 ) )
      throw new IllegalDataException("Number of classes must be >= 2 and <= 65534, found " + classes);
  }

  /**
   * This method has two functions:
   *   1) computes default strata sizes (can be overriden by user)
   *   2) identifies unbalanced classes (based on stratas) and extracts them out of the dataset.
   *
   * @param ary
   * @param strata
   */
  public void extractMinorities(ValueArray ary, Map<Integer,Integer> strata){
    _nHist = MinorityClasses.histogram(ary, _classcol);
    _gHist = MinorityClasses.globalHistogram(_nHist);
    final int num_nodes = H2O.CLOUD.size();
    final long num_chunks = ary.chunks();
    HashSet<H2ONode> nodes = new HashSet();
    for( long i=0; i<num_chunks; i++ ) {
      nodes.add(ary.getChunk(i)._h2o);
      if( nodes.size() == num_nodes ) // All of them?
        break;                        // Done
    }
    int cloudSize = nodes.size();
    int [] nodesIdxs = new int[nodes.size()];
    int k = 0;
    for(H2ONode n:nodes)nodesIdxs[k++] = n.index();
    Arrays.sort(nodesIdxs);
    int majority = 0;
    for(int i:_gHist)if(i > majority)majority = i;
    majority = Math.round((_sample*majority)/cloudSize);
    int minStrata = majority >> 9;
    _strata = new int[_gHist.length];
    for(int i = 0; i < _strata.length; ++i){
      // TODO should class weight be adjusted?
      _strata[i] = Math.min(_gHist[i],Math.max(minStrata,Math.round((_sample*_gHist[i])/cloudSize)));
    }

    if( strata != null) for(Map.Entry<Integer, Integer> e: strata.entrySet())
      if(e.getKey() < 0 || e.getKey() >= _strata.length)
        System.err.println("Ignoring illegal class index when parsing strata argument: " + e.getKey());
      else
        _strata[e.getKey()] = Math.min(_gHist[e.getKey()], e.getValue());

    for(int i:nodesIdxs){
      if(_gHist[i] < (int)(_strata[i]/_sample))System.err.println("There is not enough samples of class " + i + ".");
    }
    // decide which classes need to be extracted
    SortedSet<Integer> uClasses = new TreeSet<Integer>();
    for(int i:nodesIdxs)
      for(int c = 0; c < _nHist[i].length; ++c)
        // node does not have enough samples
        if(_nHist[i][c] < _strata[c])uClasses.add(c);
    if(!uClasses.isEmpty()){
      int [] u  = new int [uClasses.size()];
      int i = 0;
      for(int c:uClasses)u[i++] = c;
      _uClasses = MinorityClasses.extractUnbalancedClasses(ary, _classcol, u);
    }

  }
  public static DRF web_main( ValueArray ary, int ntrees, int depth, float sample, short binLimit, StatType stat, long seed, int classcol, int[] ignores, Key modelKey, boolean parallelTrees, double[] classWt, int features, boolean stratify, Map<Integer,Integer> strata) {
    // Make a Task Key - a Key used by all nodes to report progress on RF
    DRF drf = new DRF();
    assert features==-1 || ((features>0) && (features<ary._cols.length-1));
    drf._features = features;
    drf._parallel = parallelTrees;
    drf._ntrees = ntrees;
    drf._depth = depth;
    drf._stat = stat.ordinal();
    drf._arykey = ary._key;
    drf._classcol = classcol;
    drf._treeskey = Key.make("Trees of " + ary._key,(byte)1,Key.KEY_OF_KEYS);
    drf._seed = seed;
    drf._ignores = ignores;
    drf._modelKey = modelKey;
    assert 0.0f <= sample && sample <= 1.0f;
    drf._sample = sample;
    drf._binLimit = binLimit;
    drf._classWt = classWt;
    drf.validateInputData(ary);
    drf._t_main = new Timer();

    DKV.put(drf._treeskey, new Value(drf._treeskey, 4)); //4 bytes for the key-count, which is zero
    Column c = ary._cols[classcol];
    final int classes = (short)((c._max - c._min)+1);
    UKV.put(modelKey, new Model(modelKey,drf._treeskey,ary._cols.length,classes,sample,ary._key,ignores,drf._features, ntrees));

    DKV.write_barrier();
    drf.fork(drf._arykey);
    return drf;
  }

  /**
   * Job to inhale data for stratify sampling
   * (done differently than standard inhale since we sort the data by classes)
   *
   * @author tomasnykodym
   *
   */
  static final class DataInhale extends RecursiveAction {
    int _classcol;
    int [] binColIds;
    int [] rawColIds;
    int [] rawColMins;
    boolean [] iclasses;
    DataAdapter _dapt;
    int nclasses;
    int [] _startRows;
    Key _k;
    ValueArray _ary;
    boolean _bin;

    public DataInhale(){

    }
    public DataInhale(DataInhale other){
      _classcol = other._classcol;
      binColIds = other.binColIds;
      rawColIds = other.rawColIds;
      rawColMins = other.rawColMins;
      iclasses = other.iclasses;
      _dapt = other._dapt;
      _startRows = other._startRows;
      _k = other._k;
      _ary = other._ary;
      _bin = other._bin;
      nclasses = other.nclasses;
    }
    @Override
    protected void compute() {
      AutoBuffer bits = _ary.getChunk(_k);
      Column cl = _ary._cols[_classcol];
      int rows = bits.remaining()/_ary.rowSize();
      int [] indexes = new int[nclasses];
      ROWS:for(int i = 0; i < rows; ++i){
        int c = (int)(_ary.datad(bits, i, cl)-cl._min);
        int outputRow = indexes[c] + _startRows[c];
        if( (iclasses != null) && iclasses[c] )               continue ROWS;
        for( int col:binColIds) if( _ary.isNA(bits, i, col) ) continue ROWS;
        for( int col:rawColIds) if( _ary.isNA(bits, i, col) ) continue ROWS;
        ++indexes[c];
        if(_bin){
          for(int col:binColIds)
            _dapt.addValueRaw((float)_ary.datad(bits, i, col),outputRow,col);
        } else {
          for(int col:binColIds)
            _dapt.addValue((float)_ary.datad(bits, i, col), outputRow, col);
          for(int col = 0; col < rawColIds.length; ++col){
            _dapt.addValue((short)(_ary.data(bits, i, rawColIds[col]) - rawColMins[col]), outputRow, rawColIds[col]);
          }
        }
      }
      _bin = false;
    }
  }

  /**
   * Data inhale for stratify sampling.
   *
   * Sorts input by class column and can sample
   * data from other nodes in case of minority/unbalanced class.
   *
   * @return
   */
  private DataAdapter inhaleData() {
    final ValueArray ary = ValueArray.value(_arykey);
    int row_size = ary.rowSize();
    int rpc = (int)ValueArray.CHUNK_SZ/row_size;
    final Column classCol = ary._cols[_classcol];
    final int nclasses = (int)(classCol._max - classCol._min + 1);
    boolean [] unbalancedClasses = null;

    final int [][] chunkHistogram = new int [_keys.length+1][nclasses];

    // first compute histogram of classes per chunk (for sorting purposes)
    RecursiveAction [] htasks = new RecursiveAction[_keys.length];
    for(int i = 0; i < _keys.length; ++i){
      final int chunkId = i;
      final Key chunkKey = _keys[i];
      htasks[i] = new RecursiveAction() {
        @Override
        protected void compute() {
          AutoBuffer bits = ary.getChunk(chunkKey);
          int rows = bits.remaining()/ary.rowSize();
          for(int i = 0; i < rows; ++i)
            ++chunkHistogram[chunkId][(int)(ary.data(bits, i, classCol)-classCol._min)];
        }
      };
    }
    invokeAll(htasks);
    // compute sums of our class counts
    for(int i = 0; i < _keys.length; ++i)
      for(int j = 0; j < nclasses; ++j)
        chunkHistogram[_keys.length][j] += chunkHistogram[i][j];

    ArrayList<Key> myKeys = new ArrayList<Key>();
    for(Key k:_keys)myKeys.add(k);
    if(_uClasses != null) {
      // boolean array to keep track which classes to ignore when reading local keys
      unbalancedClasses = new boolean[nclasses];
      for(UnbalancedClass c:_uClasses){
        unbalancedClasses[c._c] = true;
        int nrows = _strata[c._c];
        int echunks = 1 + nrows/rpc;
        if(echunks >= c._chunks.length) { // we need all the chuks from all the nodes
          chunkHistogram[_keys.length][c._c] = _gHist[c._c];
          for(Key k:c._chunks)
            myKeys.add(k);
        } else { // sample only from some of chunks on other nodes
          int r = 0;
          ArrayList<Integer> indexes = new ArrayList<Integer>();
          // include all local chunks and identify non-locals
          for(int i = 0; i < c._chunks.length; ++i) {
            if(c._chunks[i].home()){
              myKeys.add(c._chunks[i]);
              r += DKV.get(c._chunks[i])._max/row_size;
            } else
              indexes.add(i);
          }
          // sample from non-local chunks until we have enough rows
          // sampling only works on chunk boundary -> we can end up with upt to rpc more rows than requested
          Random rand = new Random(_seed);
          while(r < nrows){
            assert !indexes.isEmpty();
            int i = rand.nextInt() % indexes.size();
            Key k = c._chunks[indexes.get(i)];
            r += DKV.get(k)._max/row_size;
            myKeys.add(k);
            int last = indexes.size()-1;
            indexes.set(i, indexes.get(last));
            indexes.remove(last);
          }
          chunkHistogram[_keys.length][c._c] = Math.min(r,nrows);
        }
      }
    }

    int totalRows = 0; // size of local DataAdapter
    for(int i = 0; i < nclasses;++i)
      totalRows += chunkHistogram[_keys.length][i];
    final DataAdapter dapt = new DataAdapter(ary, _classcol, _ignores, totalRows, ValueArray.getChunkIndex(_keys[0]), _seed, _binLimit, _classWt);

    // vector keeping track of indexes of individual classes so that we can read data in parallel
    final int [] startRows = new int[nclasses];

    dapt.initIntervals(nclasses);
    for(int i = 1; i < nclasses; ++i){
      startRows[i] = startRows[i-1] + chunkHistogram[_keys.length][i-1];
      dapt.setIntervalStart(i, startRows[i]);
    }
    // cols that do not need binning
    int [] rawCols = new int[ary.numCols() - _ignores.length];
    // cols that will be binned
    int [] binCols = new int[ary.numCols() - _ignores.length];
    int b = 0;
    int r = 0;

    for(int i = 0; i < ary.numCols(); ++i){
      if(Arrays.binarySearch(_ignores, i) < 0){
        if(dapt.binColumn(i)) binCols[b++] = i;
        else rawCols[r++] = i;
      }
    }
    rawCols = Arrays.copyOf(rawCols, r);
    binCols = Arrays.copyOf(binCols, b);
    int [] rawMins = new int[r];
    for(int i = 0; i < rawCols.length; ++i)
      rawMins[i] = (int)ary._cols[rawCols[i]]._min;
    ArrayList<DataInhale> inhaleJobs = new ArrayList<DRF.DataInhale>();
    for (int i = 0; i < myKeys.size(); ++i){
      Key k = myKeys.get(i);
      DataInhale job = new DataInhale();
      job._ary = ary;
      job._dapt = dapt;
      job._classcol = _classcol;
      job._startRows = startRows.clone();
      job._k = k;
      job._bin = (binCols.length > 0);
      job.iclasses = i < _keys.length?unbalancedClasses:null;
      job.binColIds = binCols;
      job.rawColIds = rawCols;
      job.rawColMins = rawMins;
      job.nclasses = nclasses;
      inhaleJobs.add(job);
      if(i < _keys.length) // local (majority class) chunk
        for(int j = 0; j < nclasses; ++j){
          if(unbalancedClasses == null || !unbalancedClasses[j])
            startRows[j] += chunkHistogram[i][j];
      } else { // chunk containing only single unbalanced class
        // find the unbalanced class
        int c = 0;
        for(;c < (_uClasses.length-1) && i-_uClasses[c]._chunks.length >= 0; ++c);
        startRows[c] += DKV.get(myKeys.get(i))._max/rpc;
      }
    }
    invokeAll(inhaleJobs);
    // compute the binning
    if(binCols.length > 0){
      ArrayList<RecursiveAction> binningJobs = new ArrayList<RecursiveAction>();
      for(int c:binCols){
        final int col = c;
        binningJobs.add(new RecursiveAction() {
          @Override
          protected void compute() {
            dapt.computeBins(col);
          }
        });
      }
      invokeAll(binningJobs);
      // now do the inhale jobs again, this time reading all the values
      ArrayList<DataInhale> inhaleJobs2 = new ArrayList<DRF.DataInhale>();
      for(DataInhale job: inhaleJobs)
        inhaleJobs2.add(new DataInhale(job));
      invokeAll(inhaleJobs2);
    }
    return dapt;
  }


  private static void binData(final DataAdapter dapt, final Key [] keys, final ValueArray ary, final int [] colIds, final int ncols){
    final int rowsize= ary._rowsize;
    ArrayList<RecursiveAction> jobs = new ArrayList<RecursiveAction>();
    int start_row = 0;
    for(final Key k:keys) {
      if( !k.home() ) continue;
      final int rows = DKV.get(k)._max/rowsize;
      final int S = start_row;
      jobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          AutoBuffer bits = ary.getChunk(k);
          ROWS: for(int j = 0; j < rows; ++j) {
            for(int c = 0; c < ncols; ++c)
              if( ary.isNA(bits,j,colIds[c])) continue ROWS;
            for(int c = 0; c < ncols; ++c)
              dapt.addValueRaw((float)ary.datad(bits,j,colIds[c]), j + S, colIds[c]);
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
    final ValueArray ary = ValueArray.value(DKV.get(arykey));
    final int rowsize = ary._rowsize;
    _numrows = DKV.get(keys[0])._max/rowsize; // Rows-per-chunk

    // One pass over all chunks to compute max rows
    Timer t_max = new Timer();
    int num_rows = 0;
    long unique = -1;
    for( Key key : keys )
      if( key.home() ) {
        num_rows += DKV.get(key)._max/rowsize;
        if( unique == -1 )
          unique = ValueArray.getChunkIndex(key);
      }
    Utils.pln("[RF] Max/min done in "+ t_max);

    Timer t_bin = new Timer();
    // The data adapter...
    final DataAdapter dapt = new DataAdapter(ary, _classcol, _ignores, num_rows, unique, _seed, _binLimit, _classWt);

    // Check that we have proper number of valid columns vs. features selected, if not cap
    int validCols = -1; // for classIdx column
    for (int i = 0; i < dapt.columns(); ++i)
      if (!dapt.ignore(i)) ++validCols;
    if (validCols < _features) {
      System.out.println("Limiting features from "+_features+" to "+validCols+" because there are no more valid columns in the dataset");
      _features = validCols;
    }

    // Now load the DataAdapter with all the rows on this Node
    final int ncolumns = ary._cols.length;

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
          AutoBuffer bits = ary.getChunk(k);
          ROWS: for(int j = 0; j < rows; ++j) {
            for(int c = 0; c < ncolumns; ++c) // Bail out of broken rows in not-ignored columns
              if( !icols[c] && ary.isNA(bits,j,c)) {
                dapt.setBad(S+j);
                continue ROWS;
              }
            for( int c = 0; c < ncolumns; ++c)
              if( icols[c] )
                dapt.addValue((short)0,S+j,c);
              else if( dapt.binColumn(c) ) {
                dapt.addValue((float)ary.datad(bits,j,c), S+j, c);
              } else {
                long v = ary.data(bits,j,c);
                v -= ary._cols[c]._min;
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
    DataAdapter dapt = _useStratifySampling?inhaleData():extractData(_arykey, _keys);
    Utils.pln("[RF] Data adapter built in " + t_extract );
    Data t = Data.make(dapt);
    _validation = t; // FIXME... this does not look right.

    // Figure the number of trees to make locally, so the total hits ntrees.
    // Divide equally amongst all the nodes that actually have data.
    // First: compute how many nodes have data.
    ValueArray ary = ValueArray.value(DKV.get(_arykey));
    final long num_chunks = ary.chunks();
    final int num_nodes = H2O.CLOUD.size();
    HashSet<H2ONode> nodes = new HashSet();
    for( long i=0; i<num_chunks; i++ ) {
      nodes.add(ary.getChunkKey(i).home_node());
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

    if (_features==-1){
      int used = -1; // we don't use the class column, but it is not ignored
      for(int i = 0; i < t.columns(); ++i) if(!t.ignore(i)) ++used;
      _features = (int)Math.sqrt(used);
    }

    // Make a single RandomForest to that does all the tree-construction work.
    Utils.pln("[RF] Building "+ntrees+" trees");
    _rf = new RandomForest(this, t, ntrees, _depth, 0.0, StatType.values()[_stat],_parallel,_features,_ignores);
    tryComplete();
  }

  public void reduce( DRemoteTask drt ) { }
}
