package hex.rf;
import hex.rf.MinorityClasses.UnbalancedClass;
import hex.rf.Tree.StatType;

import java.util.*;

import jsr166y.RecursiveAction;
import water.*;
import water.Timer;
import water.ValueArray.Column;

/** Distributed RandomForest */
public final class DRF extends water.DRemoteTask {

  // ---------------
  // OPTIONS FOR RF
  // ---------------
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
// FIXME this variable is not use
//  /** Global histogram class x frequency */
//  int [][] _histogram;

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
    assert numSplitFeatures==-1 || ((numSplitFeatures>0) && (numSplitFeatures<ary._cols.length-1)) : "Bad number of split features: expected -1 or a number in range (0, num of cols)";
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

    // Pre-process data in case of stratified sampling: extract minorities
    if(drf._useStratifySampling) {
      drf.extractMinorities(ary, strata);
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
    DataAdapter dapt = DABuilder.create(this).build(_arykey, _keys);
    Utils.pln("[RF] Data adapter built in " + t_extract );
    // Prepare data and compute missing parameters.
    Data t            = Data.make(dapt);
    _numSplitFeatures = howManySplitFeatures(t);
    System.err.println("_numSplitFeatures = " + _numSplitFeatures);
    int ntrees        = howManyTrees();

    Utils.pln("[RF] Building "+ntrees+" trees");
    new RandomForest(this, t, ntrees, _depth, 0.0, StatType.values()[_stat],_parallel,_numSplitFeatures,_ignores);
    tryComplete();
  }

  public final void reduce( DRemoteTask drt ) { }

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
  private void extractMinorities(ValueArray ary, Map<Integer,Integer> strata) {
    // Compute class histogram per node.
    int[][] _nHist = MinorityClasses.histogram(ary, _classcol);
    // Compute global histogram.
    _gHist = MinorityClasses.globalHistogram(_nHist);
    final int num_nodes = H2O.CLOUD.size();
    final long num_chunks = ary.chunks();
    HashSet<H2ONode> nodes = new HashSet();
    for( long i=0; i<num_chunks; i++ ) {
      H2ONode nod = ary.getChunk(i)._h2o;
      // FIXME this is adhoc solution
      nodes.add(nod != null ? nod : H2O.CLOUD.SELF);
      if( nodes.size() == num_nodes ) // All of them?
        break;                        // Done
    }
    int cloudSize = nodes.size();
    int [] nodesIdxs = new int[nodes.size()];
    int k = 0;
    for(H2ONode n:nodes) nodesIdxs[k++] = n.index();
    Arrays.sort(nodesIdxs);
    // Get count of rows with majority class over all nodes.
    int majority = 0;
    for(int i : _gHist) if(i > majority) majority = i;
    // Recompute respecting sampling rate and spread of data over nodes.
    majority = Math.round((_sample*majority)/cloudSize);
    // FIXME: Compute minimal strata.
    int minStrata = majority >> 9;
    _strata = new int[_gHist.length];
    for(int i = 0; i < _strata.length; ++i){
      // TODO should class weight be adjusted?
      // Compute required number of rows with class <i> on each node.
      int expClassNumPerNode = Math.round((_sample * _gHist[i])/cloudSize);
      // This is a required number of rows of given class <i> on each node.
      _strata[i] = Math.min(_gHist[i],Math.max(minStrata, expClassNumPerNode));
    }

    // Now consider strata specified by the user and recompute expected numbers
    // of rows per node and per class.
    if( strata != null) for(Map.Entry<Integer, Integer> e: strata.entrySet()) {
      int clsIdx = e.getKey();
      int clsVal = e.getValue();
      if(clsIdx < 0 || clsIdx >= _strata.length)
        Utils.pln("Ignoring illegal class index when parsing strata argument: " + e.getKey());
      else
        _strata[clsIdx] = Math.min(_gHist[clsIdx], clsVal);
    }

    for(int i : nodesIdxs) {
      if( _gHist[i] < (int)(_strata[i]/_sample) )
        Utils.pln("There is not enough samples of class " + i + ".");
    }
    // Decide which classes need to be extracted
    SortedSet<Integer> uClasses = new TreeSet<Integer>();
    for(int i:nodesIdxs) {
      for(int c = 0; c < _nHist[i].length; ++c) {
        // Node does not have enough samples
        if(_nHist[i][c] < _strata[c])uClasses.add(c);
      }
    }

    if(!uClasses.isEmpty()) {
      int [] u  = new int [uClasses.size()];
      int i = 0;
      for(int c:uClasses)u[i++] = c;
      _uClasses = MinorityClasses.extractUnbalancedClasses(ary, _classcol, u);
    }
  }
}
