package hex.rf;

import java.util.*;

import water.*;

/**
 * A model is an ensemble of trees that can be serialized and that can be used
 * to classify data.
 */
public class Model extends RemoteTask {

  /** Key model is stored in the cloud with */
  public Key _key;
  /** Number of trees in the model. */
  private int      _ntrees;
  /** Compressed tree data. */
  public byte[][]  _trees;
  /** Number of response classes in the source data. */
  public int       _classes;
  /** Number of features these trees are built for */
  public int       _features;
  /** Sampling rate used when building trees. */
  public float     _sample;
  /** Columns ignored in the model. */
  public int[]     _ignoredColumns;

  /** A RandomForest Model
   * @param treeskey    a key of keys of trees
   * @param classes     the number of response classes
   * @param data        the dataset
   */
  public Model() { }
  public Model(Key key, Key treeskey, int features, int classes, float sample, int[] ignoredColumns) {
    _key = key;
    _classes = classes;
    _features = features;
    _sample = sample;
    _ignoredColumns = ignoredColumns;
    Key[] tkeys = treeskey.flatten(); // Trees
    if( tkeys == null ) return;       // Broken model?  quit now
    _trees = new byte[tkeys.length][];
    for( int i = 0; i < tkeys.length; i++ ) {
      Value v = DKV.get(tkeys[i]);
      if( v == null )   return;     // Broken model; quit now
      _trees[i] = v.get();
    }
    _ntrees = tkeys.length;
  }

  /** The number of trees in this model. */
  public int treeCount() { return _ntrees; }
  public int size() { return _ntrees; }

  public String name(int atree) {
    if( atree == -1 ) atree = size();
    assert atree <= size();
    return _key.toString()+"["+atree+"]";
  }

  /**
   * Classify a row according to one particular tree.
   * @param tree_id  the number of the tree to use
   * @param chunk    the chunk we are using
   * @param row      the row number in the chunk
   * @param rowsize  the size in byte of each row
   * @return the predicted response class, or class+1 for broken rows
   */
  private short classify0(int tree_id, byte[] chunk, int row, int rowsize, ValueArray data, int[]offs, int[]size, int[]base, int[]scal ) {
    return Tree.classify(_trees[tree_id], data, chunk, row, rowsize, offs, size, base, scal, (short)_classes);
  }

  private void vote(byte[] chunk, int row, int rowsize, ValueArray data, int[]offs, int[]size, int[]base, int[]scal, int[] votes, int[][] ignores, int skipCount ) {
    assert votes.length == _classes+1/*+1 to catch broken rows*/;
    TREE:
    for( int i = 0; i < _ntrees; i++ ) {
     int[] ignore = ignores[i];
     if (ignore!=null) {
       int row_id = row -skipCount;
       for(int v :ignore) if (v==row_id) continue TREE; else if (v>row_id) break;
     }
     votes[classify0(i, chunk, row, rowsize, data, offs, size, base, scal)]++;
    }
  }

  public short classify(byte[] chunk, int row, int rowsize, ValueArray data, int[]offs, int[]size, int[]base, int[]scal, int[] votes, double[] classWt, Random rand, int[][]ignores, int skipCount ) {
    // Vote all the trees for the row
    vote(chunk, row, rowsize, data, offs, size, base, scal, votes, ignores, skipCount);
    // Scale the votes by class weights: it as-if rows of the weighted classes
    // were replicated many times so get many votes.
    if( classWt != null )
      for( int i=0; i<votes.length-1; i++ )
        votes[i] = (int)(votes[i]*classWt[i]);
    // Tally results
    int result = 0;
    int tied = 1;
    for( int i = 1; i<votes.length-1; i++)
      if( votes[i] > votes[result] ) { result=i; tied=1; }
      else if( votes[i] == votes[result] ) { tied++; }
    if( tied==1 ) return (short)result;
    // Tie-breaker logic
    int j = rand.nextInt(tied); // From zero to number of tied classes-1
    int k = 0;
    for( int i=0; i<votes.length-1; i++ )
      if( votes[i]==votes[result] && (k++ >= j) )
        return (short)i;
    throw H2O.unimpl();
  }

  /** Has this tree been built with rows in this chunk? */
  public boolean buildComplement(int tree_id, Key chunk) {
    return  Tree.getChunkMap(_trees[tree_id]).containsKey(chunk.toString());
  }


  // Lazy initialization of tree leaves, depth
  private transient Counter _tl, _td;

  /** Internal computation of depth and number of leaves. */
  public void find_leaves_depth() {
    if( _tl != null ) return;
    _td = new Counter();
    _tl = new Counter();
    for( byte[] tbits : _trees ) {
      long dl = Tree.depth_leaves(tbits);
      _td.add((int) (dl >> 32));
      _tl.add((int) dl);
    }
  }
  public String leaves() { find_leaves_depth(); return _tl.toString(); }
  public String depth()  { find_leaves_depth(); return _td.toString(); }

  private static class Counter {
    double _min = Double.MAX_VALUE, _max = Double.MIN_VALUE;
    int    _count;
    double _total;
    void add(double what) {
      _total += what;
      _min = Math.min(what, _min);
      _max = Math.max(what, _max);
      ++_count;
    }
    double avg() { return _total / _count; }
    @Override public String toString() { return _count==0 ? " / / " : String.format("%4.1f / %4.1f / %4.1f",_min,avg(),_max); }
  }

  public void invoke( Key args ) { throw H2O.unimpl(); }
  public void compute( )         { throw H2O.unimpl(); }
  /** Return the random seed used to sample this tree. */
  public int getTreeSeed(int i) {  return Tree.seed(_trees[i]); }
  public int getStart(int i, Key chunk_key) {
    HashMap<String,Integer> map = Tree.getChunkMap(_trees[i]);
    Integer start = map.get(chunk_key.toString());
    assert start != null;
    return start;
  }
  public int getEnd(int i, Key chunk_key) {
    HashMap<String,Integer> map = Tree.getChunkMap(_trees[i]);
    Integer start = map.get(chunk_key.toString());
    assert start != null;
    Integer[] ints = new Integer[map.size()];
    map.values().toArray(ints);
    Arrays.sort(ints);
    boolean next =false;
    for(int v : ints) {
      if (next) return v;
      if (v == start) next = true;
    }
    return map.get("end");
  }
}
