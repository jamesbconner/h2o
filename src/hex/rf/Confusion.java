package hex.rf;

import java.util.*;
import water.*;

/**
 * Confusion Matrix.  Incrementally computes a Confusion Matrix for a
 * KEY_OF_KEYS of Trees, vs a given input dataset.  The set of Trees can grow
 * over time.  Each request from the Confusion compute on any new trees (if
 * any), and report a matrix.  Cheap if all trees already computed.
 */
public class Confusion extends MRTask {

  // A KEY_OF_KEYS of Trees, that may incrementally grow over time.
  // 0 <= _ntrees < _ntrees0 <= #keys_in_treeskey <= _maxtrees;
  public Key _treeskey;            // KEY_OF_KEYS of Trees; this may grow over time
  public int _ntrees;              // Trees processed so far
  public int _ntrees0;             // Trees being processed *this pass*
  public int _maxtrees;            // Expected final tree max

  /** Tree Keys */
  transient public Key[] _tkeys;
  /** Tree data */
  transient public byte[][] _trees;

  // Dataset we are building the matrix on.  The classes must be in the last
  // column, and the column count must match the Trees.
  public Key _datakey;              // The dataset key
  transient public ValueArray _data; // The dataset array
  public int _N;                    // Number of classes


  // The Confusion Matrix - a NxN matrix of [actual] -vs- [predicted] classes,
  // referenced as _matrix[actual][predicted].  Each row in the dataset is
  // voted on by all trees, and the majority vote is the predicted class for
  // the row.  Each row thus gets 1 entry in the matrix.
  public long _matrix[][]; // A _N x _N matrix of classes

  // An array of tree-votes, 1 16-bit short per class per row.  Each count is
  // what class a single tree voted on this row.  The max class is how the
  // ensemble votes this row.  Limit of 64K trees.  This costs e.g. the poker
  // dataset 2 bytes times 10 classes or 20 bytes per row, while poker itself
  // only uses 11 bytes per row - so 2x MORE memory.  While for, e.g. covtype,
  // the cost is 2x7 classes or 14 bytes per row on top of 64 bytes per row for
  // the data for an overhead of only 18%.
  public Key _votes;
  transient public Key[] _vkeys;

  /** For reproducibility we can control the randomness in the computation of the
      confusion matrix. The default seed when deserializing is 42.*/
  transient Random _rand = new Random(42);

  private transient Model _model;

  public Confusion() {}  // Constructor for use by the serializers


  public Confusion( Key treeskey, ValueArray data, int maxtrees, int seed ) {
    _ntrees = 0;
    _ntrees0 = -1;              // Not set yet; need to count keys in _treeskey
    _maxtrees = maxtrees;
    _treeskey = treeskey;
    _rand = new Random(seed);
    // Do some basic validation on the dataset.
    _datakey = data._key;
    int nchunks = (int)data.chunks();
    int num_cols = data.num_cols();
    int min = (int)data.col_min(num_cols-1); // Typically 0-(n-1) or 1-N
    int max = (int)data.col_max(num_cols-1);
    _N = max-min+1;             // Range of last column is #classes
    assert _N > 0;

    _model = new Model(_treeskey,(short) _N , data);

    // Now the incremental-voting array.  Implemented as a K/V pair per
    // original dataset chunk, with 2xN bytes per V and each K homed to the
    // same home as the original chunk - and only updated by the original CPU.
    // We just make the keys here; the values are only made on the homes.
    _votes = Key.make("Votes of "+treeskey,(byte)1,Key.KEY_OF_KEYS);
    _vkeys = new Key[nchunks];

    byte[] bits = new byte[8*nchunks];
    int off = 0;
    off = UDP.set4(bits,off,nchunks);  // Count of keys
    for( int i = 0; i<nchunks; i++ ) { // Write them out
      // Home of the original dataset chunk
      H2ONode home = data.make_chunkkey(((long)i)<<ValueArray.LOG_CHK).home_node();
      // New vote-key homed to the same place
      byte[] kb = new byte[16];
      UUID uuid = UUID.randomUUID();
      UDP.set8(kb, 0,uuid.getLeastSignificantBits());
      UDP.set8(kb, 8,uuid. getMostSignificantBits());
      Key k = Key.make(kb,(byte)0,Key.DFJ_INTERNAL_USER,home);
      _vkeys[i] = k;
      while( k.wire_len() + off > bits.length )
        bits = Arrays.copyOf(bits,bits.length<<1);
      off = k.write(bits,off);
    }
    DKV.put(_votes, new Value(_votes,Arrays.copyOf(bits,off)));
    shared_init();
  }

  // Shared init: for new Confusions, for remote Confusions
  private void shared_init() {
    // Wire-len passed the dataset key, go ahead and load the dataset's
    // ValueArray - it's Schema-on-Read
    _data = (ValueArray)DKV.get(_datakey);
    // Flatten the key-of-X-keys into an array-of-X-keys
    _tkeys = _treeskey.flatten(); // Trees
    _vkeys = _votes.flatten();    // Votes-per-row
    // If we got passed _ntree0, then process that number of trees.
    // Else process all available trees.
    if( _ntrees0 == -1 ) _ntrees0 = _tkeys.length;

    // For the trees, further flatten to arrays of tree-bits.
    // But only both with the first ntree0 trees.
    _trees = new byte[_ntrees0][];
    for( int i=0; i<_ntrees0; i++ )
      _trees[i] = DKV.get(_tkeys[i]).get();
  }

  /** Once-per-remote invocation init.  The standard M/R framework will
      endlessly clone the original object "for free" (well, for very low cost),
      but the wire-line format does not send over things we can compute locally.
      So compute locally, once, some things we want in all cloned instances. */
  public void init() {
    super.init();
    shared_init();
  }

  /** Refresh, in case the number of trees has grown.
      During a refresh the _matrix is changing. */
  public void refresh() {
    if( _ntrees >= _tkeys.length ) // Did all available trees already?
      return;                      // Then done!
    _matrix = null;                // Erase the old partial results
    _ntrees0 = _tkeys.length;      // Lock down the number of trees being done
    // launch a M/R job to do the math
    invoke(_datakey);
    // Update the Confusion key to the larger count of voted trees
    _ntrees = _ntrees0;
    toKey();
  }

  /** Write the Confusion to its key. */
  public Key toKey() {
    Stream s = new Stream(wire_len());
    write(s);
    Key key = Key.make("ConfusionMatrix of "+_datakey);
    DKV.put(key, new Value(key, s._buf));
    return key;
  }

  public static Confusion fromKey( Key key ) {
    Confusion c = new Confusion();
    c.read(new Stream(DKV.get(key).get()));
    c.shared_init();
    return c;
  }

  /** A classic Map/Reduce style incremental computation of the confusion matrix on a chunk of data. */
  public void map( Key chunk_key ) {
    byte[] chunk_bits = DKV.get(chunk_key).get();      // Get the raw dataset bits of the chunk
    final int rowsize = _data.row_size();
    final int rows = chunk_bits.length/rowsize;
    final int num_cols = _data.num_cols();
    final int ccol = num_cols-1;               // Column holding the class
    final int cmin = (int)_data.col_min(ccol); // Typically 0-(n-1) or 1-N

    // Get the existing votes
    int nchk = ValueArray.getChunkIndex(chunk_key);
    Key vkey = _vkeys[nchk];
    Value votes = DKV.get(vkey);
    if( votes == null ) votes = new Value(vkey,rows*_N*2);
    else assert votes._max == rows*_N*2;
    byte[] vbits = votes.get();
    int[] ties = null;          // Tie-breaker used only when needed
    // Make an empty confusion matrix for this chunk
    _matrix = new long[_N][_N];

    // Now for all rows, classify & vote!
    MAIN_LOOP :
    for( int i=0; i<rows; i++ ) {
      for( int k=0; k<num_cols; k++ )
        if( !_data.valid(chunk_bits,i,rowsize,k) ) continue MAIN_LOOP; // Skip broken rows

      // For all trees on this row, vote!
      int vidx = i*2*_N;        // Vote index
      for( int t=_ntrees; t<_ntrees0; t++ ) {
        // This tree's prediction for row i
        int predict = Tree.classify(_trees[t],_data,chunk_bits,i,rowsize);
       // int predict2 = _model.classify(t, chunk_bits, i, rowsize);
      //  assert predict==predict2;
        assert 0<= predict && predict < _N : ("prediction "+predict+" < "+_N);
        // Bump the row's class-vote
        UDP.add2(vbits,vidx+(predict<<1),1);
      }
      // Get the new vote for the row, the new predicted class
      int predict = -1;          // Class-index with the max vote
      int predvs  = -1;          // Votes for the predicted class
      boolean tie=false;
      int vsum = 0;
      for( int n=0; n<_N; n++ ){ // Find max vote / predicted-class-for-row
        int cvotes = UDP.get2(vbits,vidx+(n<<1));
        vsum += cvotes;
        if( cvotes > predvs ) { predvs = cvotes; predict = n; tie=false; }
        else if( cvotes == predvs ) tie=true;
      }
      assert vsum == _ntrees0 : "vsum="+vsum+" ntrees0="+_ntrees0; // neither under-nor-over counting
      // If we have any class-vote-ties on the final tree, break them randomly.
      // Prevents biasing e.g. towards the lower class numbers.
      if( tie && _ntrees0 == _maxtrees ) {
        if( ties == null ) ties = new int[_N];
        for( int n=0; n<_N; n++ ) // Find max vote / predicted-class-for-row
          ties[n] = UDP.get2(vbits,vidx+(n<<1));
        predict = Utils.maxIndex(ties,_rand);
      }
      // Find the current row class
      int cclass = (int)_data.data(chunk_bits,i,rowsize,ccol) - cmin;
      assert 0<= cclass && cclass < _N : ("cclass "+cclass+" < "+_N);
      // Bump the confusion matrix
      _matrix[cclass][predict]++;
    }

    // Save the votes for a rainy day, or clean them up if done
    if( _ntrees0 == _maxtrees )   DKV.remove(vkey);
    else                          DKV.put(vkey,new Value(vkey,vbits));
  }

  /** Reduction combines the confusion matrices. */
  public void reduce( DRemoteTask drt ) {
    Confusion C = (Confusion)drt;
    long[][] m1 = _matrix;
    long[][] m2 = C._matrix;
    if( m1 == null ) { _matrix = m2; return; } // Take other work straight-up
    for( int i=0; i<m1.length; i++ )
      for( int j=0; j<m1.length; j++ )
        m1[i][j] += m2[i][j];
  }
}
