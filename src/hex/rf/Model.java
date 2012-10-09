package hex.rf;

import java.io.*;
import java.util.Random;
import java.util.logging.Level;

import water.*;

/**
 * A model is an ensemble of trees that can be serialized and that can be used
 * to
 */
public class Model implements Serializable {

  /** Number of trees in the model. */
  final private int                  _ntrees;
  /** Compressed tree data. */
  final private byte[][]             _trees;
  /** Number of response classes in the source data. */
  final private short                _classes;
  /** The dataset */
  final transient private ValueArray _data;
  /** Pseudo random number used as tie breaker. */
  final private Random               _rand = new Random(42); // FIXME:
                                                             // parameterize?
  final transient private Key        _treeskey;

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
  public Model(Key treeskey, short classes, ValueArray data) {
    _classes = classes;
    _data = data;
    _treeskey = treeskey;
    Key[] _tkeys = _treeskey.flatten(); // Trees
    _ntrees = _tkeys.length;
    _trees = new byte[_ntrees][];
    for( int i = 0; i < _ntrees; i++ )
      _trees[i] = DKV.get(_tkeys[i]).get();
  }

  /** The number of trees in this model. */
  public int treeCount() {
    return _ntrees;
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
  public short classify(int tree_id, byte[] chunk, int row, int rowsize) {
    short predict = Tree.classify(_trees[tree_id], _data, chunk, row, rowsize);
    assert 0 <= predict && predict < _classes : ("prediction " + predict
        + " < " + _classes);
    return predict;
  }

  public int[] vote(byte[] chunk, int row, int rowsize) {
    int[] votes = new int[_classes];
    for( int i = 0; i < _ntrees; i++ )
      votes[classify(i, chunk, row, rowsize)]++;
    return votes;
  }

  public short classify(byte[] chunk, int row, int rowsize) {
    int[] votes = vote(chunk, row, rowsize);
    return (short) Utils.maxIndex(votes, _rand);
  }

  /** Are there new trees? */
  public boolean refreshNeeded() {
    return _treeskey.flatten().length > _ntrees;
  }

  public int size() {
    return _ntrees;
  }

  private Counter tl, td;

  /** Internal computation of depth and number of leaves. */
  private void compute() {
    if( tl != null ) return;
    td = new Counter();
    tl = new Counter();
    for( byte[] tbits : _trees ) {
      long dl = Tree.depth_leaves(tbits);
      td.add((int) (dl >> 32));
      tl.add((int) dl);
    }
  }

  public String leaves() {
    compute();
    return tl.toString();
  }

  public String depth() {
    compute();
    return td.toString();
  }

  public void save(String filename) {
    try {
      ObjectOutput out = new ObjectOutputStream(new BufferedOutputStream( new FileOutputStream(filename)));
      try {
        out.writeObject(this);
      } finally {
        out.close();
      }
    } catch( IOException e ) { throw new Error(e); }

  }

  public static Model load(String filename) {
    try{
      ObjectInput input = new ObjectInputStream ( new BufferedInputStream( new FileInputStream( filename ) ) );
      try{
        return (Model)input.readObject();
      } finally {  input.close(); }
    } catch( Exception e){ throw new Error(e); }
  }

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

    public double min() {
      return min_;
    }

    public double max() {
      return max_;
    }

    public double avg() {
      return total_ / count_;
    }

    public int count() {
      return count_;
    }

    @Override
    public String toString() {
      return avg() + " (" + min_ + " ... " + max_ + ")";
    }
  }
}
