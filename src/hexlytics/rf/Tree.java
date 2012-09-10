package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Statistic.Split;

import java.io.*;
import java.util.UUID;

import jsr166y.CountedCompleter;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveTask;
import water.*;

public class Tree extends CountedCompleter {
  ThreadLocal<BaseStatistic>[] stats_;
  static public enum StatType { ENTROPY, NEW_ENTROPY, GINI };

  final StatType _type;
  final Data _data;
  final int _data_id; // Data-subset identifier (so trees built on this subset are not validated on it)
  final int _max_depth;
  final double _min_error_rate;
  public INode _tree;
  long _timeToBuild; // Time needed to build the tree
  int _features;

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate, StatType stat, int features ) {
    _data = data;
    _data_id = data.data_._data_id;
    _max_depth = max_depth;
    _min_error_rate = min_error_rate;
    _type = stat;
    _features = features;
  }
  // Constructor used to inhaling/de-serializing a pre-built tree.
  public Tree( int data_id ) {
    _data = null;
    _data_id = data_id;
    _max_depth = 0;
    _min_error_rate = -1.0;
    _type = StatType.ENTROPY;
  }

  /** Determines the error rate of a single tree. */
  public double validate(Data data) {
    double errors = 0, total = 0;
    for (Row row: data) {
      total += row.weight();
      if (row.classOf() != classify(row)) errors += row.weight();
    }
    return errors/total;
  }

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  private void createStatistics() {
    // Change this to a different amount of statistics, for each possible subnode one, 2 for binary trees
    stats_ = new ThreadLocal[2];
    for (int i = 0; i < stats_.length; ++i) stats_[i] = new ThreadLocal<BaseStatistic>();
  }

  private void freeStatistics() { stats_ = null; } // so that they can be GCed
static int KK;
  // Actually build the tree
  public void compute() {
    System.out.println(KK++);
    createStatistics();
    if (_type == StatType.ENTROPY) computeNumeric(); else compute2();
    String st = "";//toString();
    System.out.println("Tree ("+_data_id+") d="+_tree.depth()+" l="+_tree.leaves()+"  "+ ((st.length() < 120) ? st : (st.substring(0, 120)+"...")));
    tryComplete();
    freeStatistics();
  }

  void computeNumeric() { // All rows in the top-level split
    Statistic s = new Statistic(_data,null,_features);
    for (Row r : _data) s.add(r);
    _tree = new FJEntropyBuild(s,_data,0).compute();
  }

  // TODO this has to change a lot, only a temp working version
  private BaseStatistic getOrCreateStatistic(int index, Data data) {
    BaseStatistic result = stats_[index].get();
    if (result==null) {
     result = (_type == StatType.GINI) ? new GiniStatistic(data,_features) : new EntropyStatistic(data,_features);
     stats_[index].set(result);
    }
    result.reset(data);
    return result;
  }


  void compute2() {
    // first get the statistic so that it can be reused
    BaseStatistic left = getOrCreateStatistic(0,_data);
    // calculate the split
    for (Row r : _data)  left.add(r);
    BaseStatistic.Split spl = left.split();
    if (spl.isLeafNode())  _tree = new LeafNode(spl.split);
    else  _tree = new FJBuild(spl,_data,0).compute();
  }

  private class FJEntropyBuild extends RecursiveTask<INode> {

    static final boolean THREADED = false;  // multi-threaded ?

    final Statistic _s;         // All the rows that this split munged over
    final Data _data;           // The resulting 1/2-sized dataset from the above split
    final int _d;               // depth

    FJEntropyBuild( Statistic s, Data data, int depth ) { _s = s; _data = data; _d = depth; }
    public INode compute() {
      // terminate the branch prematurely
      if(_max_depth > 0 &&  _d >= _max_depth )// FIXME...  || _s.error() < _min_error_rate )
        return new LeafNode(_s.classOf());
      if (_s.singleClass()) return new LeafNode(_s.classOf());
      Split best = _s.best();
      if (best == null) return new LeafNode(_s.classOf());
      Node nd = new Node(best.column,best.value,_data);
      Data[] res = new Data[2];
      Statistic[] stats = new Statistic[] { new Statistic(_data,_s, _s._features), new Statistic(_data,_s, _s._features)};
      _data.filter(best,res,stats);
      if (THREADED) {
        ForkJoinTask<INode> fj0 = new FJEntropyBuild(stats[0],res[0],_d+1).fork();
        nd._r =                   new FJEntropyBuild(stats[1],res[1],_d+1).compute();
        nd._l = fj0.join();
      } else {
        nd._l = new FJEntropyBuild(stats[0],res[0],_d+1).compute();
        nd._r = new FJEntropyBuild(stats[1],res[1],_d+1).compute();
      }
      return nd;
    }
  }

  private class FJBuild extends RecursiveTask<INode> {
    final BaseStatistic.Split split_;
    final Data data_;
    final int depth_;

    FJBuild(BaseStatistic.Split split, Data data, int depth) {
      this.split_ = split;
      this.data_ = data;
      this.depth_ = depth;
    }

    @Override public INode compute() {
      BaseStatistic left = getOrCreateStatistic(0,data_);       // first get the statistics
      BaseStatistic right = getOrCreateStatistic(1,data_);
      Data[] res = new Data[2];       // create the data, node and filter the data
      SplitNode nd = new SplitNode(split_.column, split_.split);
      data_.filter(nd._column, nd._split,res,left,right);
      BaseStatistic.Split ls = left.split();      // get the splits
      BaseStatistic.Split rs = right.split();
      if (ls.isLeafNode())  nd._l = new LeafNode(ls.split);      // create leaf nodes if any
      if (rs.isLeafNode())  nd._r = new LeafNode(rs.split);
      if ((nd._l == null) && (nd._r == null)) {   // calculate the missing subnodes as new FJ tasks, join if necessary
        ForkJoinTask<INode> fj0 = new FJBuild(ls,res[0],depth_+1).fork();
        nd._r = new FJBuild(rs,res[1],depth_+1).compute();
        nd._l = fj0.join();
      } else if (nd._l == null)   nd._l = new FJBuild(ls,res[0],depth_+1).compute();
      else if (nd._r == null)     nd._r = new FJBuild(rs,res[1],depth_+1).compute();
      return nd;
    }

  }

  public static abstract class INode {
    protected INode() { }
    abstract int classify(Row r);
    public abstract int depth();
    public abstract int leaves();
    public abstract int nodes();

    public abstract void print(TreePrinter treePrinter) throws IOException;
    abstract void write( DataOutputStream dos ) throws IOException;
    static INode read( DataInputStream dis ) throws IOException {
      int b = dis.readByte();
      switch( b ) {
      case '[': return  LeafNode.read(dis); // Leaf selector
      case '(': return      Node.read(dis); // Node selector
      case 'S': return SplitNode.read(dis); // Node selector
      default:
        throw new Error("Misformed serialized rf.Tree; expected to find an INode tag but found '"+(char)b+"' instead");
      }
    }
  }

  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {
    final int class_;    // A category reported by the inner node
    LeafNode(int c) { class_ = c;  }
    @Override public int depth()  { return 0; }
    @Override public int leaves() { return 1; }
    @Override public int nodes()  { return 1; }

    public int classify(Row r) { return class_; }
    public String toString()   { return "["+class_+"]"; }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( DataOutputStream dos ) throws IOException {
      assert Short.MIN_VALUE <= class_ && class_ < Short.MAX_VALUE;
      dos.writeByte('[');       // Leaf indicator
      dos.writeShort(class_);
    }
    static LeafNode read( DataInputStream dis ) throws IOException {
      return new LeafNode(dis.readShort());
    }
  }

  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  static class Node extends INode {
    final Data _data;
    final int _column;
    final double _value;
    INode _l, _r;
    public Node(int column, double value, Data data) {
      _column= column;
      _value = value;
      _data  = data;
    }
    public int classify(Row row) { return (row.getS(_column)<=_value ? _l : _r).classify(row);  }

    @Override public int depth()  { return 1 + Math.max(_l.depth(), _r.depth()); }
    @Override public int leaves() { return _l.leaves() + _r.leaves(); }
    @Override public int nodes()  { return 1 + _l.nodes() + _r.nodes(); }

    // Computes the original split-value, as a float.
    private final float split_value() { return  _data.unmap(_column,(float) _value); } 

    public String toString() {
      return _data.columnNames()[_column] +"<" + Utils.p2d(split_value()) + " ("+_l+","+_r+")";
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( DataOutputStream dos ) throws IOException {
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      dos.writeByte('(');       // Node indicator
      dos.writeShort(_column);
      dos.writeShort((short)_value);// Pass the short index instead of the actual value
      _l.write(dos);
      _r.write(dos);
    }
    static Node read( DataInputStream dis ) throws IOException {
      int col = dis.readShort();
      int idx = dis.readShort(); // Read the short index instead of the actual value
      Node n = new Node(col,((double)idx)+0.5,null);
      n._l = INode.read(dis);
      n._r = INode.read(dis);
      return n;
    }
   }

  /** Gini classifier node.
   */
  static class SplitNode extends INode {
    final int _column;
    final int _split;
    INode _l, _r;

    @Override int classify(Row r) {
      return r.getColumnClass(_column) <= _split ? _l.classify(r) : _r.classify(r);
    }

    public SplitNode(int column, int split) {
      this._column = column;
      this._split = split;
    }

    public void set(int direction, INode n) { if (direction==0) _l=n; else _r=n; }
    @Override public int depth()  { return 1 + Math.max(_l.depth(), _r.depth()); }
    @Override public int leaves() { return _l.leaves() + _r.leaves(); }
    @Override public int nodes()  { return 1 + _l.nodes() + _r.nodes(); }

    public String toString() {
      return "S "+_column +"<=" + _split + " ("+_l+","+_r+")";
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }

    void write( DataOutputStream dos ) throws IOException {
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      dos.writeByte('S');       // Node indicator
      dos.writeShort(_column);
      dos.writeShort((short)_split);// Pass the short index instead of the actual value
      _l.write(dos);
      _r.write(dos);
    }
    static SplitNode read( DataInputStream dis ) throws IOException {
      int col = dis.readShort();
      int idx = dis.readShort(); // Read the short index instead of the actual value
      SplitNode n = new SplitNode(col,idx);
      n._l = INode.read(dis);
      n._r = INode.read(dis);
      return n;
    }
  }
  public int classify(Row r) { return _tree.classify(r); }
  public String toString()   { return _tree.toString(); }

  // Write the Tree to a random Key homed here.
  public Key toKey() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      DataOutputStream dos = new DataOutputStream(bos);
      dos.writeInt(_data_id);
      _tree.write(dos);
    } catch( IOException e ) { throw new Error(e); }
    Key key = Key.make(UUID.randomUUID().toString(),(byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(key,new Value(key,bos.toByteArray()));
    return key;
  }
  public static Tree fromKey( Key key ) {
    byte[] bits = DKV.get(key).get();
    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bits));
      Tree t = new Tree(dis.readInt());
      t._tree = INode.read(dis);
      return t;
    } catch( IOException e ) { throw new Error(e); }
  }
}
