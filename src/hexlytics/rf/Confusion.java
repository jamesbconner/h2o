package hexlytics.rf;

import java.io.*;
import java.util.Arrays;
import java.util.UUID;
import java.util.Random;
import water.*;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializationManager;
import water.serialization.RemoteTaskSerializer;

/**
 * Confusion Matrix.  Incrementally computes a Confusion Matrix for a
 * KEY_OF_KEYS of Trees, vs a given input dataset.  The set of Trees can grow
 * over time.  Each request from the Confusion compute on any new trees (if
 * any), and report a matrix.  Cheap if all trees already computed.
 * @author cliffc
 */
@RTSerializer(Confusion.Serializer.class)
public class Confusion extends MRTask {

  // A KEY_OF_KEYS of Trees, that may incrementally grow over time.
  // 0 <= _ntrees < _ntrees0 <= #keys_in_treeskey <= _maxtrees;
  public Key _treeskey;         // KEY_OF_KEYS of Trees; this may grow over time
  public int _ntrees;           // Trees processed so far
  public int _ntrees0;          // Trees being processed *this pass*
  public int _maxtrees;         // Expected final tree max

  // Tree Keys
  public Key[] _tkeys;          // Array of Tree-Keys
  public byte[][] _tbits;       // Array of Tree bytes

  // Dataset we are building the matrix on.  The classes must be in the last
  // column, and the column count must match the Trees.
  public Key _arykey;           // The dataset key
  public ValueArray _ary;       // The dataset array
  public int _N;                // Number of classes

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
  public Key[] _vkeys;

  public Confusion( Key treeskey, ValueArray ary, int maxtrees ) {
    _ntrees = 0;                // Processed so far
    _ntrees0 = -1;              // Not set yet; need to count keys in _treeskey
    _maxtrees = maxtrees;       // Max number desired.
    // Key of Tree-Keys
    _treeskey = treeskey;

    // Do some basic validation on the dataset.
    // The classes are in the last column.
    _arykey = ary._key;
    int nchunks = (int)ary.chunks();
    int num_cols = ary.num_cols();
    int min = (int)ary.col_min(num_cols-1); // Typically 0-(n-1) or 1-N
    int max = (int)ary.col_max(num_cols-1);
    _N = max-min+1;             // Range of last column is #classes
    assert _N > 0;

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
      H2ONode home = ary.make_chunkkey(((long)i)<<ValueArray.LOG_CHK).home_node();
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
    // Finish off with the shared init
    shared_init();
  }
  // Private no-arg constructor for use by the serializers
  private Confusion() {}

  public static class Serializer extends RemoteTaskSerializer<Confusion> {
    @Override public int wire_len(Confusion c) { 
      return 
        4+                               // _ntrees
        4+                               // _ntrees0
        4+                               // _maxtrees
        4+                               // _N
        1+                               // _matrix flag
        (c._matrix==null?0:c._N*c._N*8)+ // _matrix
        c._treeskey.wire_len()+          // _treeskey
        c._arykey  .wire_len()+          // _arykey
        c._votes   .wire_len()+          // _votes
        0;
    }
    @Override public int write( Confusion c, byte[] buf, int off ) {
      off += UDP.set4(buf,off,c._ntrees);
      off += UDP.set4(buf,off,c._ntrees0);
      off += UDP.set4(buf,off,c._maxtrees);
      off += UDP.set4(buf,off,c._N);
      buf[off++] = (byte)((c._matrix == null)?0:1);
      if( c._matrix != null )
        for( int i=0; i<c._N; i++ )
          for( int j=0; j<c._N; j++ )
            off += UDP.set8(buf,off,c._matrix[i][j]);
      off = c._treeskey.write(buf,off);
      off = c._arykey  .write(buf,off);
      off = c._votes   .write(buf,off);
      return off;
    }
    @Override public Confusion read( byte[] buf, int off ) { 
      Confusion c = new Confusion();
      c._ntrees  = UDP.get4(buf,(off+=4)-4);
      c._ntrees0 = UDP.get4(buf,(off+=4)-4);
      c._maxtrees= UDP.get4(buf,(off+=4)-4);
      c._N       = UDP.get4(buf,(off+=4)-4);
      if( buf[off++] == 1 ) {
        c._matrix = new long[c._N][c._N];
        for( int i=0; i<c._N; i++ )
          for( int j=0; j<c._N; j++ )
            c._matrix[i][j] = UDP.get8(buf,(off+=8)-8);
      }
      c._treeskey= Key.read(buf,off);  off += c._treeskey.wire_len();
      c._arykey  = Key.read(buf,off);  off += c._arykey  .wire_len();
      c._votes   = Key.read(buf,off);  off += c._votes   .wire_len();
      return c;
    }
    @Override public void write( Confusion t, DataOutputStream dos ) { throw new Error("do not call"); }
    @Override public Confusion read ( DataInputStream dis ) { throw new Error("do not call"); }
  }

  // Shared init: for new Confusions, for remote Confusions
  private void shared_init() {
    // Wire-len passed the dataset key, go ahead and load the dataset's
    // ValueArray - it's Schema-on-Read
    _ary = (ValueArray)DKV.get(_arykey);
    // Flatten the key-of-X-keys into an array-of-X-keys
    _tkeys = _treeskey.flatten(); // Trees
    _vkeys = _votes.flatten();    // Votes-per-row
    // If we got passed _ntree0, then process that number of trees.
    // Else process all available trees.
    if( _ntrees0 == -1 ) _ntrees0 = _tkeys.length;

    // For the trees, further flatten to arrays of tree-bits.
    // But only both with the first ntree0 trees.
    _tbits = new byte[_ntrees0][];
    for( int i=0; i<_ntrees0; i++ )
      _tbits[i] = DKV.get(_tkeys[i]).get();
  }

  // Once-per-remote invocation init.  The standard M/R framework will
  // endlessly clone the original object "for free" (well, for very low cost),
  // but the wire-line format does not send over things we can compute locally.
  // So compute locally, once, some things we want in all cloned instances.
  public void init() {
    super.init();
    shared_init();
  }

  // Refresh, in case the number of trees has grown.
  // During a refresh the _matrix is changing.
  public void refresh() {
    if( _ntrees >= _tkeys.length ) // Did all available trees already?
      return;                      // Then done!
    _matrix = null;                // Erase the old partial results
    _ntrees0 = _tkeys.length;      // Lock down the number of trees being done
    // launch a M/R job to do the math
    invoke(_arykey);
    // Update the Confusion key to the larger count of voted trees
    _ntrees = _ntrees0;
    toKey();
  }

  public String toString() {
    throw new Error("unimplemented");
  }

  // Write the Confusion to a random Key
  public Key toKey() {
    RemoteTaskSerializer sc = RemoteTaskSerializationManager.get(Confusion.class);
    byte[] buf = new byte[sc.wire_len(this)];
    sc.write(this,buf,0);
    Key key = Key.make("ConfusionMatrix of "+_arykey);
    DKV.put(key,new Value(key,buf));
    return key;
  }
  public static Confusion fromKey( Key key ) {
    RemoteTaskSerializer sc = (RemoteTaskSerializationManager.get(Confusion.class));
    Confusion c = (Confusion)sc.read(DKV.get(key).get(),0);
    c.shared_init();            // Shared init
    return c;
  }

  // A classic Map/Reduce style incremental computation of the confusion matrix.
  public void map( Key key ) {
    // Get the raw dataset bits to work on
    byte[] dbits = DKV.get(key).get();
    final int rowsize = _ary.row_size();
    final int rows = dbits.length/rowsize;
    final int num_cols = _ary.num_cols();
    final int ccol = num_cols-1;              // Column holding the class
    final int cmin = (int)_ary.col_min(ccol); // Typically 0-(n-1) or 1-N

    // Get the existing votes
    int nchk = ValueArray.getChunkIndex(key);
    Key vkey = _vkeys[nchk];
    Value votes = DKV.get(vkey);
    if( votes == null ) votes = new Value(vkey,rows*_N*2);
    else assert votes._max == rows*_N*2;
    byte[] vbits = votes.get();
    int[] ties = null;          // Tie-breaker used only when needed
    Random rand = null;

    // Make an empty confusion matrix for this chunk
    _matrix = new long[_N][_N];

    // Now for all rows, classify & vote!
    for( int i=0; i<rows; i++ ) {
      boolean valid = true;
      for( int k=0; k<num_cols; k++ )
        if( !_ary.valid(dbits,i,rowsize,k) ) 
          valid = false;
      if( valid == false ) continue; // Skip broken rows

      // For all trees on this row, vote!
      int vidx = i*2*_N;        // Vote index
      for( int t=_ntrees; t<_ntrees0; t++ ) {
        // This tree's prediction for row i
        int predict = Tree.classify(_tbits[t],_ary,dbits,i,rowsize);
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
        if( rand == null ) rand = new Random();
        for( int n=0; n<_N; n++ ) // Find max vote / predicted-class-for-row
          ties[n] = UDP.get2(vbits,vidx+(n<<1));
        predict = Utils.maxIndex(ties,rand);
      }
      // Find the current row class
      int cclass = (int)_ary.data(dbits,i,rowsize,ccol) - cmin;
      assert 0<= cclass && cclass < _N : ("cclass "+cclass+" < "+_N);
      // Bump the confusion matrix
      _matrix[cclass][predict]++;
    }

    // Save the votes for a rainy day, or clean them up if done
    if( _ntrees0 == _maxtrees ) { // Done with last tree?
      DKV.remove(vkey);           // Remove all votes
    } else {                      // Else need to save partial vote results
      DKV.put(vkey,new Value(vkey,vbits));
    }
  }

  private final long dumap() {
    long sum=0;
    for( int i=0; i<_matrix.length; i++ )
      for( int j=0; j<_matrix.length; j++ )
        sum += _matrix[i][j];
    return sum;
  }

  // Reduction just combines the confusion matrices
  public void reduce( DRemoteTask drt ) {
    Confusion C = (Confusion)drt;
    long[][] m1 = _matrix;
    long[][] m2 = C._matrix;
    if( m1 == null ) {          // No local work?
      _matrix = m2;             // Take other work straight-up
    } else {
      for( int i=0; i<m1.length; i++ )
        for( int j=0; j<m1.length; j++ )
          m1[i][j] += m2[i][j];
    }
  }
}
