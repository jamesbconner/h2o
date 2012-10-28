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
  public short classify(int tree_id, byte[] chunk, int row, int rowsize, ValueArray data, int[]offs, int[]size, int[]base, int[]scal ) {
    short predict = Tree.classify(_trees[tree_id], data, chunk, row, rowsize, offs, size, base, scal);
    assert 0 <= predict && predict < _classes : ("prediction " + predict
        + " < " + _classes);
    return predict;
  }

  public int[] vote(byte[] chunk, int row, int rowsize, ValueArray data, int[]offs, int[]size, int[]base, int[]scal ) {
    int[] votes = new int[_classes];
    for( int i = 0; i < _ntrees; i++ )
      votes[classify(i, chunk, row, rowsize, data, offs, size, base, scal)]++;
    return votes;
  }

  public short classify(byte[] chunk, int row, int rowsize, ValueArray data, int[]offs, int[]size, int[]base, int[]scal ) {
    int[] votes = vote(chunk, row, rowsize, data, offs, size, base, scal);
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
    double _min = Double.MAX_VALUE;
    double _max = Double.MIN_VALUE;
    int    _count;
    double _total;
    public void add(double what) {
      _total += what;
      _min = Math.min(what, _min);
      _max = Math.max(what, _max);
      ++_count;
    }
    public double min() { return _min; }
    public double max() { return _max; }
    public double avg() { return _total / _count; }
    public int count()  { return _count; }
    @Override
    public String toString() { return _count==0 ? " / / " : String.format("%4.1f / %4.1f / %4.1f",_min,avg(),_max); }
  }

  public void invoke( Key args ) { throw H2O.unimpl(); }
  public void compute( )         { throw H2O.unimpl(); }
}
