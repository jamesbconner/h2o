package hex.rf;

import hex.rf.Utils.Counter;

import java.util.Random;

import water.*;

public class Model {

  /** Number of trees in the model.*/
  final private int _ntrees;
  /** Compressed tree data. */
  final private byte[][] _trees;
  /** Number of response classes in the source data. */
  final private short _classes;
  /** The dataset */
  final private ValueArray _data;
  /** Pseudo random number used as tie breaker. */
  final private Random _rand = new Random(42);  // FIXME: parameterize?
  final private Key _treeskey;

  /** A RandomForest Model
   * @param treeskey  a key of keys of trees
   * @param classes   the number of response classes
   * @param data      the dataset
   */
  public Model( Key treeskey , short classes, ValueArray data ) {
    _classes = classes;
    _data = data;
    _treeskey = treeskey;
    Key[] _tkeys = _treeskey.flatten(); // Trees
    _ntrees = _tkeys.length;
    _trees = new byte[_ntrees][];
    for( int i=0; i<_ntrees; i++ ) _trees[i] = DKV.get(_tkeys[i]).get();
  }

  /** The number of trees in this model. */
  public int treeCount() { return _ntrees; }


  /** Classify a row according to one particular tree.
   * @param tree_id  the number of the tree to use
   * @param chunk    the chunk we are using
   * @param row      the row number in the chunk
   * @param rowsize  the size in byte of each row
   * @return the predicted response class
   */
  public short classify(int tree_id,  byte[] chunk, int row, int rowsize) {
    short predict = Tree.classify(_trees[tree_id], _data, chunk, row, rowsize);
    assert 0<= predict && predict < _classes : ("prediction "+predict+" < "+_classes);
    return predict;
  }

  public int[] vote( byte[] chunk, int row, int rowsize) {
    int[] votes = new int[_classes];
    for (int i=0;i<_ntrees;i++)
      votes[classify(i,chunk,row,rowsize)]++;
    return votes;
  }

  public short classify(byte[]chunk, int row, int rowsize) {
    int[] votes = vote(chunk, row, rowsize);
    return (short) Utils.maxIndex(votes, _rand);
  }

  /** Are there new trees? */
  public boolean refreshNeeded() {
    return _treeskey.flatten().length > _ntrees;
  }

  public int size() { return _ntrees; }

  private Counter tl, td;
  /** Internal computation of depth and number of leaves. */
  private void compute() {
    td = new Counter(); tl = new Counter();
    for( byte[] tbits : _trees ) {
      long dl = Tree.depth_leaves(tbits);
      td.add((int) (dl >> 32));
      tl.add((int) dl);
    }
  }

  public String leaves() {
    if (tl==null) compute();
    return tl.toString();
  }

  public String depth() {
    if (td==null) compute();
    return td.toString();
  }

}
