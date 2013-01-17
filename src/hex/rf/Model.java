package hex.rf;

import java.util.Arrays;
import java.util.Random;

import water.*;
import water.util.Counter;

/**
 * A model is an ensemble of trees that can be serialized and that can be used
 * to classify data.
 */
public class Model extends Iced implements Cloneable {
  /** Key model is stored in the cloud with */
  public Key _key;
  /** Number of response classes in the source data. */
  public int       _classes;
  /** Number of features these trees are built for */
  public int       _features;
  /** Sampling rate used when building trees. */
  public float     _sample;
  /** Key model was built with */
  public Key       _dataset;
  /** Columns ignored in the model. */
  public int[]     _ignoredColumns;
  /** Number of split features */
  public int       _splitFeatures;

  /** Number of keys the model expects to be built for it */
  public int       _totalTrees;
  /** All the trees in the model */
  public Key[]     _tkeys;

  /** A RandomForest Model
   * @param treeskey    a key of keys of trees
   * @param classes     the number of response classes
   * @param data        the dataset
   */
  public Model(Key key, Key[] tkeys, int features, int classes, float sample, Key dataset, int[] ignoredColumns, int splitFeatures, int totalTrees) {
    _key            = key;
    _classes        = classes;
    _features       = features;
    _sample         = sample;
    _dataset        = dataset;
    _ignoredColumns = ignoredColumns;
    _splitFeatures  = splitFeatures;
    _totalTrees     = totalTrees;
    _tkeys          = tkeys;
    for( Key tkey : _tkeys ) assert DKV.get(tkey)!=null;
  }

  /** Empty constructor for deserialization */
  public Model() { }

  static public Model make(Model old, Key tkey) {
    try {
      Model m = (Model)old.clone();
      m._tkeys = Arrays.copyOf(old._tkeys,old._tkeys.length+1);
      m._tkeys[m._tkeys.length-1] = tkey;
      return m;
    } catch( CloneNotSupportedException cnse ) { throw H2O.unimpl();/*cannot happen*/ }
  }

  /** The number of trees in this model. */
  public int treeCount() { return _tkeys.length; }
  public int size()      { return _tkeys.length; }

  public String name(int atree) {
    if( atree == -1 ) atree = size();
    assert atree <= size();
    return _key.toString()+"["+atree+"]";
  }

  /** Return the bits for a particular tree */
  public byte[] tree( int tree_id ) {
    return DKV.get(_tkeys[tree_id]).get();
  }

  /** Bad name, I know.  But free all internal tree keys. */
  public void deleteKeys() {
    for( Key k : _tkeys )
      UKV.remove(k);
  }

  /**
   * Classify a row according to one particular tree.
   * @param tree_id  the number of the tree to use
   * @param chunk    the chunk we are using
   * @param row      the row number in the chunk
   * @param rowsize  the size in byte of each row
   * @return the predicted response class, or class+1 for broken rows
   */
  public short classify0(int tree_id, AutoBuffer chunk, int row, int rowsize, ValueArray data ) {
    return Tree.classify(new AutoBuffer(tree(tree_id)), data, chunk, row, rowsize, (short)_classes);
  }

  private void vote(AutoBuffer chunk, int row, int rowsize, ValueArray data, int[] votes ) {
    assert votes.length == _classes+1/*+1 to catch broken rows*/;
    for( int i = 0; i < size(); i++ )
      votes[classify0(i, chunk, row, rowsize, data)]++;
  }

  public short classify(AutoBuffer chunk, int row, int rowsize, ValueArray data, int[] votes, double[] classWt, Random rand ) {
    // Vote all the trees for the row
    vote(chunk, row, rowsize, data, votes);
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

  // The seed for a given tree
  long seed( int ntree ) {
    return UDP.get8(tree(ntree),4);
  }

  // Lazy initialization of tree leaves, depth
  private transient Counter _tl, _td;

  /** Internal computation of depth and number of leaves. */
  public void find_leaves_depth() {
    if( _tl != null ) return;
    _td = new Counter();
    _tl = new Counter();
    for( Key tkey : _tkeys ) {
      long dl = Tree.depth_leaves(new AutoBuffer(DKV.get(tkey).get()));
      _td.add((int) (dl >> 32));
      _tl.add((int) dl);
    }
  }
  public Counter leaves() { find_leaves_depth(); return _tl; }
  public Counter depth()  { find_leaves_depth(); return _td; }

  /** Return the random seed used to sample this tree. */
  public long getTreeSeed(int i) {  return Tree.seed(tree(i)); }

}
