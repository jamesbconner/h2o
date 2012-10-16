package hex.rf;

import java.util.Random;

import water.*;

/**
 * A model is an ensemble of trees that can be serialized and that can be used
 * to
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
  /** Pseudo random number used as tie breaker. */
  final transient private Random     _rand = new Random(42); // FIXME:
                                                             // parameterize?
  /**
   * A RandomForest Model
   *
   * @param treeskey
   *          a key of keys of trees
   * @param classes
   *          the number of response classes
   * @param data
   *          the dataset
   */
  public Model() { }
  public Model(Key key, Key treeskey, int features, int classes) {
    _key = key;
    _classes = classes;
    _features = features;
    Key[] tkeys = treeskey.flatten(); // Trees
    _ntrees = tkeys.length;
    _trees = new byte[_ntrees][];
    for( int i = 0; i < _ntrees; i++ )
      _trees[i] = DKV.get(tkeys[i]).get();
  }

  /** The number of trees in this model. */
  public int treeCount() { return _ntrees; }
  public int size() { return _ntrees; }

  public String name() {
    return _key.toString()+"["+size()+"]";
  }

  /**
   * Classify a row according to one particular tree.
   *
   * @param tree_id
   *          the number of the tree to use
   * @param chunk
   *          the chunk we are using
   * @param row
   *          the row number in the chunk
   * @param rowsize
   *          the size in byte of each row
   * @return the predicted response class
   */
  public short classify(int tree_id, byte[] chunk, int row, int rowsize, ValueArray data) {
    short predict = Tree.classify(_trees[tree_id], data, chunk, row, rowsize);
    assert 0 <= predict && predict < _classes : ("prediction " + predict
        + " < " + _classes);
    return predict;
  }

  public int[] vote(byte[] chunk, int row, int rowsize, ValueArray data) {
    int[] votes = new int[_classes];
    for( int i = 0; i < _ntrees; i++ )
      votes[classify(i, chunk, row, rowsize, data)]++;
    return votes;
  }

  public short classify(byte[] chunk, int row, int rowsize, ValueArray data) {
    int[] votes = vote(chunk, row, rowsize, data);
    return (short) Utils.maxIndex(votes, _rand);
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

  public static class Counter {
    double min_ = Double.MAX_VALUE;
    double max_ = Double.MIN_VALUE;
    int    count_;
    double total_;
    public void add(double what) {
      total_ += what;
      min_ = Math.min(what, min_);
      max_ = Math.max(what, max_);
      ++count_;
    }
    public double min() { return min_; }
    public double max() { return max_; }
    public double avg() { return total_ / count_; }
    public int count()  { return count_; }
    @Override
    public String toString() { return count_==0 ? " / / " : String.format("%4.1f / %4.1f / %4.1f",min_,avg(),max_); }
  }

  public void invoke( Key args ) { throw H2O.unimpl(); }
  public void compute( )         { throw H2O.unimpl(); }
}
