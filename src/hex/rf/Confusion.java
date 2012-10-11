package hex.rf;

import java.util.Random;

import water.*;

/**
 * Confusion Matrix. Incrementally computes a Confusion Matrix for a KEY_OF_KEYS
 * of Trees, vs a given input dataset. The set of Trees can grow over time. Each
 * request from the Confusion compute on any new trees (if any), and report a
 * matrix. Cheap if all trees already computed.
 */
public class Confusion extends MRTask {

  /** Model used for construction of the confusion matrix. */
  public Key _modelKey;
  transient private Model _model;
  /** Dataset we are building the matrix on. The classes must be in the last
      column, and the column count must match the Trees.*/
  public Key                  _datakey;
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
  /** Number of rows used for building the matrix. */
  private long                _rows;
  /** For reproducibility we can control the randomness in the computation of the
      confusion matrix. The default seed when deserializing is 42. */
  private transient Random    _rand;
  /** The indices of the rows used for validation. This is currently mostly used
      for single node validation (the indices come from the complement of sample). */
  transient private int[]     _validation;
  /** Rows in a chunk. The last chunk is bigger, use this for all other chuncks. */
  transient private int       _rows_per_normal_chunk;

  public Confusion() { }// Constructor for use by the serializers

  private Confusion(Model model, Key datakey) {
    _modelKey = model._key;
    _datakey = datakey;
    shared_init();
  }

  // Apply a model to a dataset to produce a Confusion Matrix.  To support
  // incremental & repeated model application, hash the model & data and look
  // for that Key to already exist, returning a prior CM if one is available.
  static public Confusion make(Model model, Key datakey) {

    Key key = Key.make("ConfusionMatrix of (" + datakey+","+model.name()+")");
    Value val = UKV.get(key);
    if( val != null ) {         // Look for a prior cached result
      Confusion C = new Confusion();
      C.read(new Stream(val.get()));
      C.shared_init();
      return C;
    }

    Confusion C = new Confusion(model,datakey);
    // Compute on it: count votes
    C.invoke(datakey);

    // Output to cloud
    Stream s = new Stream(C.wire_len());
    C.write(s);
    DKV.put(key, new Value(key, s._buf));
    
    return C;
  }

  /** Shared init: for new Confusions, for remote Confusions*/
  private void shared_init() {
    _rand   = new Random(42);
    _data = (ValueArray) DKV.get(_datakey); // load the dataset
    _model = new Model();
    _model.read(new Stream(UKV.get(_modelKey).get()));
    _N = _model._classes;
    assert _N > 0;
    byte[] chunk_bits = DKV.get(_data.chunk_get(0)).get(); // get the 0-th chunk and figure out its size
    _rows_per_normal_chunk = chunk_bits.length / _data.row_size();
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
  }

  /** Set the validation set before starting. */
  public void setValidation(int[] indices) {
    if( _validation != null ) throw new Error("Confusion Matrix already initialized.");
    _validation = indices;
  }

  /** index in the validation set. */
  private int _posInValidation;

  /** Skip rows that are not in the validation set. Assumes the set is sorted in
   * ascending order.*/
  private boolean ignoreRow(int chunk_idx, int row) {
    if( _validation == null ) return false; // no validation set, use all rows.
    row = chunk_idx * _rows_per_normal_chunk + row; // adjust to get an absolute row number
    while( _validation.length > _posInValidation && _validation[_posInValidation] < row )  _posInValidation++;
    if( _validation.length == _posInValidation ) return true; // gone past the end... ignore
    return !(_validation[_posInValidation] == row); // return true if we shoud ignore the row
  }

  /**A classic Map/Reduce style incremental computation of the confusion matrix on a chunk of data. */
  public void map(Key chunk_key) {
    byte[] chunk_bits = DKV.get(chunk_key).get(); // Get the raw dataset bits
    final int rowsize = _data.row_size();
    final int rows = chunk_bits.length / rowsize;
    final int ncols= _data.num_cols();
    final int ccol = ncols - 1; // Column holding the class
    final int cmin = (int) _data.col_min(ccol); // Typically 0-(n-1) or 1-N
    int nchk = ValueArray.getChunkIndex(chunk_key);
    _matrix = new long[_N][_N]; // Make an empty confusion matrix for this chunk
    int[] votes = new int[_N];

    MAIN_LOOP: // Now for all rows, classify & vote!
    for( int i = 0; i < rows; i++ ) {
      for( int k = 0; k < ncols; k++ )
        if( !_data.valid(chunk_bits, i, rowsize, k) )  continue MAIN_LOOP; // Skip broken rows
      if( ignoreRow(nchk, i) ) continue MAIN_LOOP;
      for( int j=0; j<_N; j++ ) votes[j] = 0;
      for( int t = 0; t < _model.size(); t++ )  // This tree's prediction for row i
        votes[_model.classify(t, chunk_bits, i, rowsize, _data)]++;
      int predict = Utils.maxIndex(votes, _rand);
      int cclass = (int) _data.data(chunk_bits, i, rowsize, ccol) - cmin;
      assert 0 <= cclass && cclass < _N : ("cclass " + cclass + " < " + _N);
      _matrix[cclass][predict]++;
      _rows++;
      if( predict != cclass ) _errors++;
    }
  }

  /** Reduction combines the confusion matrices. */
  public void reduce(DRemoteTask drt) {
    Confusion C = (Confusion) drt;
    long[][] m1 = _matrix;
    long[][] m2 = C._matrix;
    if( m1 == null ) { _matrix = m2; return; } // Take other work straight-up
    for( int i = 0; i < m1.length; i++ )
      for( int j = 0; j < m1.length; j++ )  m1[i][j] += m2[i][j];
    _rows += C._rows;
    _errors += C._errors;
  }

  /** Compute the confusion matrix on the entire dataset on the current node. */
  public void mapAll() {
    Confusion tmp = new Confusion();
    for( int i = 0; i < _data.chunks(); i++ ) { map(_data.chunk_get(i)); tmp.reduce(this); }
    _matrix = tmp._matrix;
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
        + "No of variables tried at each split: " + _model._features + "\n"
        + "             Estimate of error rate: " + Math.round(err * 10000) / 100 + "%  (" + err + ")\n"
        + "                   Confusion matrix:\n"
        + confusionMatrix() + "\n"
        + "          Avg tree depth (min, max): "  + _model.depth() + "\n"
        + "         Avg tree leaves (min, max): " + _model.leaves() + "\n"
        + "                Validated on (rows): " + _rows;
    Utils.pln(s);
  }
}
