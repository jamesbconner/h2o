package hex.rf;

import java.util.Random;

import water.*;
import water.ValueArray.Column;

import com.google.common.primitives.Ints;

/**
 * Confusion Matrix. Incrementally computes a Confusion Matrix for a KEY_OF_KEYS
 * of Trees, vs a given input dataset. The set of Trees can grow over time. Each
 * request from the Confusion compute on any new trees (if any), and report a
 * matrix. Cheap if all trees already computed.
 */
public class Confusion extends MRTask {


  public int _treesUsed;
  /** Key for the model used for construction of the confusion matrix. */
  public Key _modelKey;
  /** Model used for construction of the confusion matrix. */
  transient private Model _model;
  /** Dataset we are building the matrix on.  The column count must match the Trees.*/
  public Key                  _datakey;
  /** Column holding the class, defaults to last column */
  int                         _classcol;
  /** The dataset */
  transient public ValueArray _data;
  /** Number of response classes */
  transient public int        _N;
  /** The Confusion Matrix - a NxN matrix of [actual] -vs- [predicted] classes,
      referenced as _matrix[actual][predicted]. Each row in the dataset is
      voted on by all trees, and the majority vote is the predicted class for
      the row. Each row thus gets 1 entry in the matrix.*/
  public long                 _matrix[][];
  /** Number of mistaken assignments. */
  private long                _errors;
  /** Number of rows used for building the matrix.*/
  private long                _rows;
  /** Class weights */
  private double[]            _classWt;
  /** For reproducibility we can control the randomness in the computation of the
      confusion matrix. The default seed when deserializing is 42. */
  private transient Random    _rand;
  /** Data to replay the sampling algorithm */
  transient private int[]     _chunk_row_mapping;
  private int[] _ignores;
  public boolean _computeOOB;

  /**   Constructor for use by the serializers */
  public Confusion() { }

  /** Confusion matrix
   * @param model the ensemble used to classify
   * @param datakey the key of the data that will be classified
   */
  private Confusion(Model model, Key datakey, int classcol, int[] ignores, double[] classWt, boolean computeOOB ) {
    _modelKey = model._key;
    _datakey = datakey;
    _classcol = classcol;
    _classWt = classWt != null && classWt.length > 0 ? classWt : null;
    _ignores = ignores;
    _treesUsed = model.size();
    _computeOOB = computeOOB;
    shared_init();
  }

  public Key keyFor() { return keyFor(_model._key,_model.size(),_datakey, _classcol, _computeOOB); }
  static public Key keyFor(Key modelKey, int msize, Key datakey, int classcol, boolean computeOOB) {
    return Key.make("ConfusionMatrix of (" + datakey+"["+classcol+"],"+modelKey+"["+msize+"],"+(computeOOB?"1":"0")+")");
  }

  static public Key keyForProgress(Key modelKey, int msize, Key datakey, int classcol, boolean computeOOB) {
    // make sure it is a system key
    return Key.make("\0" + "ConfusionMatrixProgress of (" + datakey+"["+classcol+"],"+modelKey+")");
  }

  public static void remove(Model model, Key datakey, int classcol, boolean computeOOB) {
    Key key = keyFor(model._key, model.size(), datakey, classcol, computeOOB);
    UKV.remove(key);
  }

  /**Apply a model to a dataset to produce a Confusion Matrix.  To support
     incremental & repeated model application, hash the model & data and look
     for that Key to already exist, returning a prior CM if one is available.*/
  static public Confusion make(Model model, Key datakey, int classcol, int[] ignores, double[] classWt,boolean computeOOB) {
    Key key = keyFor(model._key, model.size(), datakey, classcol, computeOOB);
    Confusion C = UKV.get(key, new Confusion());
    if( C != null ) {         // Look for a prior cached result
      C.shared_init();
      return C;
    }

    // mark that we are computing the matrix now
    Key progressKey = keyForProgress(model._key, model.size(), datakey, classcol, computeOOB);
    Value v = DKV.DputIfMatch(progressKey, new Value(progressKey,"IN_PROGRESS"), null, null);
    C = new Confusion(model,datakey,classcol,ignores,classWt,computeOOB);
    if (v != null) { // someone is already working on the matrix, stop
      C._matrix = null;
      return C;
    }
    if( model.size() > 0 )
      C.invoke(datakey);        // Compute on it: count votes
    UKV.put(key,C);             // Output to cloud
    UKV.remove(progressKey); // signal that we have done computing the matrix
    if( classWt != null )
      for( int i=0; i<classWt.length; i++ )
        if( classWt[i] != 1.0 )
          System.out.println("[CM] Weighted votes "+i+" by "+classWt[i]);
    return C;
  }

  public boolean isValid() {
    return _matrix != null;
  }

  /** Shared init: for new Confusions, for remote Confusions*/
  private void shared_init() {
    _rand   = new Random(42L<<32);
    _data = ValueArray.value(DKV.get(_datakey));
    _model = UKV.get(_modelKey, new Model());
    assert !_computeOOB || _model._dataset==_datakey ;
    Column c = _data._cols[_classcol];
    _N = (int)((c._max - c._min)+1);
    assert _N > 0;
  }

  /**
   * Once-per-remote invocation init. The standard M/R framework will endlessly
   * clone the original object "for free" (well, for very low cost), but the
   * wire-line format does not send over things we can compute locally. So
   * compute locally, once, some things we want in all cloned instances.
   */
  public void init() {
    super.init();
    shared_init();
    // Make a mapping from chunk# to row# just for chunks on this node
    long l = ValueArray.getChunkIndex(_keys[_keys.length-1])+1;
    _chunk_row_mapping = new int[Ints.checkedCast(l)];
    int off=0;
    for( Key k : _keys )
      if( k.home() ) {
        l = ValueArray.getChunkIndex(k);
        _chunk_row_mapping[(int)l] = off;
        off += _data.rpc(l);
      }
  }


  /**A classic Map/Reduce style incremental computation of the confusion matrix on a chunk of data. */
  public void map(Key chunk_key) {
    AutoBuffer bits = _data.getChunk(chunk_key);
    final int rowsize = _data._rowsize;
    final int rows = bits.remaining() / rowsize;
    final int cmin = (int) _data._cols[_classcol]._min;
    int nchk = (int) ValueArray.getChunkIndex(chunk_key);

    final Column[] cols = _data._cols;

    // Votes: we vote each tree on each row, holding on to the votes until the end
    int[][] votes = new int[rows][_N];

    // Build fast cutout for ignored columns
    final boolean icols[] = new boolean[cols.length];
    for( int k : _ignores ) icols[k] = true;

    // Replay the Data.java's "sample_fair" sampling algorithm to exclude data
    // we trained on during voting.

    // For all trees, re-iterate the data on this chunk
    for( int ntree = 0; ntree < _model.treeCount(); ntree++ ) {
      long seed = _model.seed(ntree);
      long init_row = _chunk_row_mapping[nchk];
      Random r = new Random(seed+(init_row<<16)  + (nchk==0?1111:0));

      // Now for all rows, classify & vote!
      ROWS: for( int i = 0; i < rows; i++ ) {
        // Bail out of broken rows in not-ignored columns
        for(int c = 0; c < cols.length; ++c)
          if( !icols[c] && _data.isNA(bits, i, cols[c])) continue ROWS;

        // Skip row used during training
        if( _computeOOB &&  r.nextFloat() < _model._sample ) continue ROWS;

        // Predict with this tree
        int prediction = _model.classify0(ntree, bits, i, rowsize, _data);
        if( prediction >= _N ) continue ROWS; // Junk row cannot be predicted
        votes[i][prediction]++; // Vote the row
      }
    }

    int validation_rows = 0;
    // Assemble the votes-per-class into predictions & score each row
    _matrix = new long[_N][_N]; // Make an empty confusion matrix for this chunk
    for( int i = 0; i < rows; i++ ) {
      int[] vi = votes[i];
      if( _classWt != null )
        for( int v = 0; v<_N; v++)
          vi[v] = (int)(vi[v]*_classWt[v]);
      int result = 0;
      int tied = 1;
      for( int l = 1; l<_N; l++)
        if( vi[l] > vi[result] ) { result=l; tied=1; }
        else if( vi[l] == vi[result] ) { tied++; }
      if( vi[result]==0 ) continue; // Ignore rows with zero votes
      if( tied>1 ) {                // Tie-breaker logic
        int j = _rand.nextInt(tied); // From zero to number of tied classes-1
        int k = 0;
        for( int l=0; l<_N; l++ )
          if( vi[l]==vi[result] && (k++ >= j) )
            { result = l; break; }
      }
      int cclass = (int) _data.data(bits, i, _classcol) - cmin;
      assert 0 <= cclass && cclass < _N : ("cclass " + cclass + " < " + _N);
      _matrix[cclass][result]++;
      if( result != cclass ) _errors++;
      validation_rows++;
    }
    _rows=Math.max(validation_rows,_rows);
  }

  /** Reduction combines the confusion matrices. */
  public void reduce(DRemoteTask drt) {
    Confusion C = (Confusion) drt;
    long[][] m1 = _matrix;
    long[][] m2 = C._matrix;
    if( m1 == null ) _matrix = m2;  // Take other work straight-up
    else {
      for( int i = 0; i < m1.length; i++ )
        for( int j = 0; j < m1.length; j++ )  m1[i][j] += m2[i][j];
    }
    _rows += C._rows;
    _errors += C._errors;
  }

  /** Text form of the confusion matrix */
  private String confusionMatrix() {
    if( _matrix == null ) return "no trees";
    final int K = _N + 1;
    double[] e2c = new double[_N];
    for( int i = 0; i < _N; i++ ) {
      long err = -_matrix[i][i];
      for( int j = 0; j < _N; j++ )   err += _matrix[i][j];
      e2c[i] = Math.round((err / (double) (err + _matrix[i][i])) * 100) / (double) 100;
    }
    String[][] cms = new String[K][K + 1];
    cms[0][0] = "";
    for( int i = 1; i < K; i++ ) cms[0][i] = "" + (i - 1); // cn[i-1];
    cms[0][K] = "err/class";
    for( int j = 1; j < K; j++ ) cms[j][0] = "" + (j - 1); // cn[j-1];
    for( int j = 1; j < K; j++ ) cms[j][K] = "" + e2c[j - 1];
    for( int i = 1; i < K; i++ )
      for( int j = 1; j < K; j++ ) cms[j][i] = "" + _matrix[j - 1][i - 1];
    int maxlen = 0;
    for( int i = 0; i < K; i++ )
      for( int j = 0; j < K + 1; j++ ) maxlen = Math.max(maxlen, cms[i][j].length());
    for( int i = 0; i < K; i++ )
      for( int j = 0; j < K + 1; j++ ) cms[i][j] = pad(cms[i][j], maxlen);
    String s = "";
    for( int i = 0; i < K; i++ ) {
      for( int j = 0; j < K + 1; j++ ) s += cms[i][j];
      s += "\n";
    }
    return s;
  }

  /** Pad a string with spaces. */
  private String pad(String s, int l) {
    String p = "";  for( int i = 0; i < l - s.length(); i++ )  p += " ";  return " " + p + s;
  }

  /** Output information about this RF. */
  public final void report() {
    double err = _errors / (double) _rows;
    String s =
          "              Type of random forest: classification\n"
        + "                    Number of trees: " + _model.size() + "\n"
        + "No of variables tried at each split: " + _model._splitFeatures + "\n"
        + "              Estimate of err. rate: " + Math.round(err * 10000) / 100 + "%  (" + err + ")\n"
        + "                   Confusion matrix:\n"
        + confusionMatrix() + "\n"
        + "          Avg tree depth (min, max): "  + _model.depth() + "\n"
        + "         Avg tree leaves (min, max): " + _model.leaves() + "\n"
        + "                Validated on (rows): " + _rows;
    Utils.pln(s);
  }

}
