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
  // OPTIONS FOR RF
  /** Total number of trees  (default: 10) */
  int _ntrees;
  /** If true, build trees in parallel (default: true) */
  boolean _parallel;
  /** Maximum depth for trees (default MaxInt) */
  int _depth;
  /** Split statistic (1=Gini, 0=Entropy; default 0) */
  int _stat;
  /** Feature holding the classifier  (default: #features-1) */
  int _classcol;
  /** Features to ignore (default: none) */
  int[] _ignores;
  /** Proportion of observations to use for building each individual tree (default: .67)*/
  float _sample;
  /** Used to replay sampling */
  int _numrows;
  /** Limit of the cardinality of a feature before we bin. */
  short _binLimit;
  /** Pseudo random seed */
  long _seed;
  /** Weights of the different features (default: 1/features) */
  double[] _classWt;
  /** Stratify sampling flag */
  boolean _useStratifySampling;
  /** Arity under which we may use exclusive splits */
  public int _exclusiveSplitLimit;
  /** Output warnings and info*/
  public int _verbose;
  /** Number of features which are tried at each split
   *
   *  If it is equal to -1 then it is computed as sqrt(num of usable columns)
   */
  int _numSplitFeatures;


  // --------------
  // INTERNAL DATA
  //--------------
  /** Key for the data being classified */
  Key _arykey;
  /** A list of unbalanced classes */
  UnbalancedClass[] _uClasses;
  /** Defined stratas for each class */
  int[] _strata;
  /** Key for the model being built */
  Key _modelKey;
  /** Key for the trees built so far*/
  Key _treeskey;
  /** Global histogram class x frequency */
  int [][] _histogram;

  //-----------------
  // Node-local data
  //-----------------
  /** Data subset to validate with locally, or NULL */
  transient Data _validation;
  /** The local RandomForest */
  transient RandomForest _rf;
  /** Main computation timer */
  transient public Timer _t_main;
  /** Global histogram over all nodes */
  transient int []   _gHist;

  //-----------------
  // Published data.
  //-----------------
  /** Number of split features.
   * It is published since its value depends on loaded data.
   */
  public final int numSplitFeatures() { return _numSplitFeatures; }
  /** Key for the training data. */
  public final Key aryKey()           { return _arykey; }

  public static DRF webMain(
      ValueArray ary,
      int ntrees, int depth, float sample, short binLimit, StatType stat,
      long seed, int classcol, int[] ignores, Key modelKey,
      boolean parallelTrees, double[] classWt, int numSplitFeatures,
      boolean stratify, Map<Integer,Integer> strata, int verbose,
      int exclusiveSplitLimit) {
    DRF drf = new DRF();
    assert numSplitFeatures==-1 || ((numSplitFeatures>0) && (numSplitFeatures<ary._cols.length-1));
    drf._numSplitFeatures = numSplitFeatures;
    drf._parallel         = parallelTrees;
    drf._ntrees           = ntrees;
    drf._depth            = depth;
    drf._stat             = stat.ordinal();
    drf._arykey           = ary._key;
    drf._classcol         = classcol;
    drf._seed             = seed;
    drf._ignores          = ignores;
    drf._modelKey         = modelKey;
    assert 0.0f <= sample && sample <= 1.0f;
    drf._sample           = sample;
    drf._binLimit         = binLimit;
    drf._classWt          = classWt;
    drf._verbose          = verbose;
    drf._exclusiveSplitLimit = exclusiveSplitLimit;
    drf._useStratifySampling = stratify;
    // Validate parameters
    drf.validateInputData(ary);
    // Start the timer.
    drf._t_main = new Timer();

    // Extract minorities
    if(drf._useStratifySampling){
      drf.extractMinorities(ary,strata);
    }

    Column c = ary._cols[classcol];
    final int classes = (short)((c._max - c._min)+1);
    UKV.put(modelKey, new Model(modelKey, new Key[0], ary._cols.length, classes, sample, ary._key, ignores, numSplitFeatures, ntrees));

    DKV.write_barrier();
    drf.fork(drf._arykey);
    return drf;
  }

  /**Class columns that are not enums are not supported as we ony do classification and not (yet) regression.
   * We require there to be at least two classes and no more than 65534. Though we will die out of memory
   * much earlier.  */
  private void validateInputData(ValueArray ary){
    Column c = ary._cols[_classcol];
    if(c._size < 0)
      throw new IllegalDataException("Regression is not supported. (class column can't be a float)");
    final int classes = (int)(c._max - c._min)+1;
    if( !(2 <= classes && classes <= 254 ) )
      throw new IllegalDataException("Number of classes must be in interval [2,254], found " + classes);
    if (0.0f > _sample || _sample > 1.0f)
      throw new IllegalDataException("Sampling rate is incorrect must be in [0,1]");
    if (_numSplitFeatures!=-1 && (_numSplitFeatures< 1 || _numSplitFeatures>=ary.numCols()))
      throw new IllegalDataException("Number of split features exceeds available data. Should be in [1,"+(ary.numCols()-1)+"]");
  }

  public static class IllegalDataException extends Error {
    public IllegalDataException(String string) { super(string); }
  }

  /**Inhale the data, build a DataAdapter and kick-off the computation.
   * */
  public final void compute() {
    Timer t_extract = new Timer();
    // Build data adapter
    DataAdapter dapt = _useStratifySampling ? inhaleData() : extractData(_arykey, _keys);
    Utils.pln("[RF] Data adapter built in " + t_extract );
    // Prepare data and compute missing parameters.
    Data t            = Data.make(dapt);
    _numSplitFeatures = howManySplitFeatures(t);
    int ntrees        = howManyTrees();

    Utils.pln("[RF] Building "+ntrees+" trees");
    new RandomForest(this, t, ntrees, _depth, 0.0, StatType.values()[_stat],_parallel,_numSplitFeatures,_ignores);
    tryComplete();
  }

  /** Unless otherwise specified each split looks at sqrt(#features). */
  private int howManySplitFeatures(Data t) {
    if (_numSplitFeatures!=-1) return _numSplitFeatures;
    int used = -1; // we don't use the class column, but it is not ignored
    for(int i = 0; i < t.columns(); ++i) if(!t.ignore(i)) ++used;
    return (int)Math.sqrt(used);
  }

  /** Figure the number of trees to make locally, so the total hits ntrees.
   * Divide equally amongst all the nodes that actually have data.
   * First: compute how many nodes have data. Give each Node ntrees/#nodes
   * worth of trees.Round down for later nodes, and round up for earlier nodes.
   */
  private int howManyTrees() {
    ValueArray ary = ValueArray.value(DKV.get(_arykey));
    final long num_chunks = ary.chunks();
    final int  num_nodes  = H2O.CLOUD.size();
    HashSet<H2ONode> nodes = new HashSet();
    for( long i=0; i<num_chunks; i++ ) {
      nodes.add(ary.getChunkKey(i).home_node());
      if( nodes.size() == num_nodes ) // All of nodes covered?
        break;                        // That means we are done.
    }

    H2ONode[] array = nodes.toArray(new H2ONode[nodes.size()]);
    Arrays.sort(array);
    // Give each H2ONode ntrees/#nodes worth of trees.  Round down for later nodes,
    // and round up for earlier nodes
    int ntrees = _ntrees/nodes.size();
    if( Arrays.binarySearch(array, H2O.SELF) < _ntrees - ntrees*nodes.size() )
      ++ntrees;

    return ntrees;
  }

  /**
   * This method has two functions:
   *   1) computes default strata sizes (can be overriden by user)
   *   2) identifies unbalanced classes (based on stratas) and extracts them out of the dataset.
   */
  public void extractMinorities(ValueArray ary, Map<Integer,Integer> strata){
    int[][] _nHist = MinorityClasses.histogram(ary, _classcol);
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
        Utils.pln("Ignoring illegal class index when parsing strata argument: " + e.getKey());
      else
        _strata[e.getKey()] = Math.min(_gHist[e.getKey()], e.getValue());

    for(int i:nodesIdxs){
      if(_gHist[i] < (int)(_strata[i]/_sample))Utils.pln("There is not enough samples of class " + i + ".");
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

  /**
   * Job to inhale data for stratify sampling.
   *
   * It is done differently than standard inhale since we sort the data by classes.
   */
  static final class DataInhale extends RecursiveAction {
    final ValueArray  _ary;
    final DataAdapter _dapt;
    final int         _classcol;
    final int[]       _startRows;
    final Key         _k;
    final boolean[]   _iclasses;
    final int[] _binColIds;
    final int[] _rawColIds;
    final int[] _rawColMins;
    final int   _nclasses;
    boolean     _bin;

    public DataInhale(final ValueArray ary, final DataAdapter dapt, int classcol,
        final int[] startRows, final Key k, boolean bin, final boolean[] iclasses,
        final int [] binColIds, final int [] rawColIds, final int [] rawColMins,
        int nclasses) {
      _ary = ary;
      _dapt = dapt;
      _classcol = classcol;
      _startRows = startRows;
      _k = k;
      _bin = bin;
      _iclasses = iclasses;
      _binColIds = binColIds;
      _rawColIds = rawColIds;
      _rawColMins = rawColMins;
      _nclasses = nclasses;

    }
    public DataInhale(DataInhale o){
      this(o._ary, o._dapt, o._classcol, o._startRows, o._k, o._bin, o._iclasses,
           o._binColIds, o._rawColIds, o._rawColMins, o._nclasses);
    }

    @Override
    protected void compute() {
      AutoBuffer bits    = _ary.getChunk(_k);
      Column     cl      = _ary._cols[_classcol];
      int        rows    = bits.remaining()/_ary.rowSize();
      int []     indexes = new int[_nclasses];

      ROWS:for(int i = 0; i < rows; ++i){
        int c = (int)(_ary.datad(bits, i, cl)-cl._min);
        int outputRow = indexes[c] + _startRows[c];

        if( (_iclasses != null) && _iclasses[c] )
          continue ROWS;
        for( int col:_binColIds)
          if( _ary.isNA(bits, i, col) ) {
            _dapt.setBad(outputRow); // FIXME I do not need to mark it bad (will be fixed in missing values fix)
            continue ROWS;
          }
        for( int col:_rawColIds)
          if( _ary.isNA(bits, i, col) ) {
            _dapt.setBad(outputRow);
            continue ROWS;
          }
        ++indexes[c];

        if(_bin){
          for(int col:_binColIds)
            _dapt.addValueRaw((float)_ary.datad(bits, i, col),outputRow,col);
        } else {
          for(int col:_binColIds)
            _dapt.addValue((float)_ary.datad(bits, i, col), outputRow, col);
          for(int col = 0; col < _rawColIds.length; ++col){
            _dapt.addValue((short)(_ary.data(bits, i, _rawColIds[col]) - _rawColMins[col]), outputRow, _rawColIds[col]);
          }
        }
      }
      _bin = false;
    }
  }

  /**
   * Data inhale for stratify sampling.
   *
   * Sorts input by the class column and sample
   * data from other nodes in case of minority/unbalanced class.
   */
  private DataAdapter inhaleData() {
    final ValueArray ary  = ValueArray.value(_arykey);
    int   row_size        = ary.rowSize();
    int   rpc             = (int)ValueArray.CHUNK_SZ/row_size;
    final Column classCol = ary._cols[_classcol];
    final int    nclasses = (int)(classCol._max - classCol._min + 1);
    boolean []   unbalancedClasses = null;

    final int [][] chunkHistogram = new int [_keys.length+1][nclasses];

    // first compute histogram of classes per chunk (for sorting purposes)
    RecursiveAction [] htasks = new RecursiveAction[_keys.length];
    for(int i = 0; i < _keys.length; ++i){
      final int chunkId  = i;
      final Key chunkKey = _keys[i];

      htasks[i] = new RecursiveAction() {
        @Override
        protected void compute() {
          AutoBuffer bits = ary.getChunk(chunkKey);
          int rows = bits.remaining()/ary.rowSize();
          for(int i = 0; i < rows; ++i) {
            if (!ary.isNA(bits, i, classCol))
              ++chunkHistogram[chunkId][(int)(ary.data(bits, i, classCol)-classCol._min)];
          }
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
        if(echunks >= c._chunks.length) { // we need all the chunks from all the nodes
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
          Random rand = Utils.getRNG(_seed);
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
    for (int i = 0; i < myKeys.size(); ++i) {
      Key k = myKeys.get(i);

      DataInhale job = new DataInhale(ary, dapt,
          _classcol,
          startRows.clone(),
          k,
          binCols.length > 0,
          i < _keys.length ? unbalancedClasses : null,
          binCols, rawCols, rawMins, nclasses);

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
      for(final int col : binCols) {
        binningJobs.add(new RecursiveAction() {
          @Override protected void compute(){
            dapt.computeBins(col);
          }
        });
      }
      invokeAll(binningJobs);

      // Now do the inhale jobs again, this time reading all the values.
      ArrayList<DataInhale> inhaleJobs2 = new ArrayList<DRF.DataInhale>();
      for(DataInhale job: inhaleJobs) {
        inhaleJobs2.add(new DataInhale(job));
      }
      invokeAll(inhaleJobs2);
    }

    return dapt;
  }


  /** Bin specified columns of given value array and inhale them into given data adapter. */
  private static void binData(final DataAdapter dapt, final Key [] keys, final ValueArray ary, final int [] colIds){
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
            for(int col : colIds)
              if( ary.isNA(bits,j,col) ) continue ROWS;
            for(int col : colIds)
              dapt.addValueRaw((float)ary.datad(bits,j,col), j + S, col);
          }
        }
      });
      start_row += rows;
    }
    invokeAll(jobs);

    // Now do binning.
    jobs.clear();
    for(final int col : colIds) {
      jobs.add(new RecursiveAction() {
        @Override protected void compute() {
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

    // Start the timer.
    Timer t_bin = new Timer();
    // Create the data adapter.
    final DataAdapter dapt = new DataAdapter( ary, _classcol, _ignores,
                                              getRowCount(keys, rowsize),
                                              getChunkId(keys),
                                              _seed, _binLimit, _classWt);

    // Check that we have proper number of valid columns vs. features selected, if not cap.
    checkAndLimitFeatureUsedPerSplit(dapt);

    // Now load the DataAdapter with all the rows on this node.
    final int ncolumns = ary._cols.length;
    int bins = 0;
    for (int i = 0; i < ncolumns; i++) if (dapt.binColumn(i)) bins++;
    int[] colIds = new int[bins];
    for (int i = 0, j = 0; i < ncolumns; i++)
      if (dapt.binColumn(i)) colIds[j++]=i;
    if (bins > 0)
      binData(dapt, keys, ary, colIds);
    // Binning is done.
    Utils.pln("[RF] Binning done in " + t_bin);

    // Inhale data.
    Timer t_inhale = new Timer();
    ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();
    int start_row = 0;
    for( final Key k : keys ) {    // now read the values
      final int S = start_row;
      if (!k.home()) continue;
      final int rows = DKV.get(k)._max/rowsize;
      dataInhaleJobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          AutoBuffer bits = ary.getChunk(k);
          ROWS: for(int j = 0; j < rows; ++j) {
            for(int c = 0; c < ncolumns; ++c) { // Bail out of broken rows in not-ignored columns
              if( !dapt.ignore(c) && ary.isNA(bits,j,c)) {
                dapt.setBad(S+j);
                continue ROWS;
              }
            }
            for( int c = 0; c < ncolumns; ++c) {
              if( dapt.ignore(c) ) {
                dapt.addValue((short)0,S+j,c);
              } else if( dapt.binColumn(c) ) {
                dapt.addValue((float)ary.datad(bits,j,c), S+j, c);
              } else {
                long v = ary.data(bits,j,c);
                v -= ary._cols[c]._min;
                dapt.addValue((short)v, S+j, c);
              }
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

  /** Check that we have proper number of valid columns vs. features selected, if not cap*/
  private void checkAndLimitFeatureUsedPerSplit(final DataAdapter dapt) {
    int validCols = -1; // for classIdx column
    for (int i = 0; i < dapt.columns(); ++i) if (!dapt.ignore(i)) ++validCols;
    if (validCols < _numSplitFeatures) {
      Utils.pln("Limiting features from "+_numSplitFeatures+" to "+validCols+" because there are no more valid columns in the dataset");
      _numSplitFeatures= validCols;
    }
  }

  /** Return the number of rows on this node. */
  private int getRowCount(final Key[] keys, final int rowsize) {
    int num_rows = 0;    // One pass over all chunks to compute max rows
    for( Key key : keys ) if( key.home() ) num_rows += DKV.get(key)._max/rowsize;
    return num_rows;
  }

  /** Return chunk index of the first chunk on this node. Used to identify the trees built here.*/
  private long getChunkId(final Key[] keys) {
    for( Key key : keys ) if( key.home() ) return ValueArray.getChunkIndex(key);
    throw new Error("No key on this node");
  }

  public void reduce( DRemoteTask drt ) { }
}
