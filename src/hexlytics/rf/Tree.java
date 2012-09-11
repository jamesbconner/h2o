package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Statistic.Split;
import java.io.*;
import java.util.UUID;
import jsr166y.*;
import water.*;

public class Tree extends CountedCompleter {
  
  public enum StatType { numeric, gini }
  
  final Data _data;             // Data source
  final int _data_id; // Data-subset identifier (so trees built on this subset are not validated on it)
  final int _max_depth;         // Tree-depth cutoff
  final double _min_error_rate; // Error rate below which a split isn't worth it
  final StatType statistic_;    // Flavor of split logic
  INode _tree;                  // Root of decision tree

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate, StatType stat ) {
    _data = data;
    _data_id = data.data_._data_id;
    _max_depth = max_depth;
    _min_error_rate = min_error_rate;
    statistic_ = stat;
  }
  // Constructor used to inhaling/de-serializing a pre-built tree.
  public Tree( int data_id ) {
    _data = null;
    _data_id = data_id;
    _max_depth = 0;
    _min_error_rate = -1.0;
    statistic_ = StatType.numeric; // Junk stat; not sensible except during building
  }

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  // Actually build the tree
  public void compute() {
    switch (statistic_) {
    case numeric: computeNumeric();  break;
    case gini:    computeGini();     break;
    default:      throw new Error("Unrecognized statistic type");
    }
    //StringBuilder sb = new StringBuilder();
    //sb.append("Tree :").append(_data_id).append(" d=").append(_tree.depth());
    //sb.append(" leaves=").append(_tree.leaves()).append("  ");
    //sb = _tree.toString(sb,999);
    //System.out.println(sb.toString());
    tryComplete();
  }
    
  void computeNumeric() {
    // All rows in the top-level split
    Statistic s = new Statistic(_data,null);
    for (Row r : _data) s.add(r);
    _tree = new FJBuild(s,_data,0).compute();
  }
  
  void computeGini() {
    // first get the statistic so that it can be reused
    GiniStatistic left = FJGiniBuild.leftStat_.get();
    if (left == null) {
      left = new GiniStatistic(_data);
      FJGiniBuild.leftStat_.set(left);
    } else {
      left.reset(_data);
    }
    // calculate the split
    for (Row r : _data) left.add(r);
    GiniStatistic.Split spl = left.split();
    if (spl.isLeafNode())
      _tree = new LeafNode(spl.split);
    else
      _tree = new FJGiniBuild(spl,_data,0).compute();
  }

  private class FJBuild extends RecursiveTask<INode> {
    final Statistic _s;         // All the rows that this split munged over
    final Data _data;           // The resulting 1/2-sized dataset from the above split
    final int _d;
    FJBuild( Statistic s, Data data, int depth ) { _s = s; _data = data; _d = depth; }
    public INode compute() {
      // terminate the branch prematurely
      if( _d >= _max_depth || _s.error() < _min_error_rate )
        return new LeafNode(_s.classOf());
      if (_s.singleClass()) return new LeafNode(_s.classOf());
      Split best = _s.best();
      if (best == null) return new LeafNode(_s.classOf());
      Node nd = new Node(best.column,best.value,_data.data_);
      Data[] res = new Data[2];
      Statistic[] stats = new Statistic[] { new Statistic(_data,_s), new Statistic(_data,_s)};
      _data.filter(best,res,stats);
      ForkJoinTask<INode> fj0 = new FJBuild(stats[0],res[0],_d+1).fork();
      nd.r_ =                   new FJBuild(stats[1],res[1],_d+1).compute();
      nd.l_ = fj0.join();
      return nd;
    }
  }
  
  private static class FJGiniBuild extends RecursiveTask<INode> {
    static ThreadLocal<GiniStatistic> leftStat_ = new ThreadLocal();
    static ThreadLocal<GiniStatistic> rightStat_ = new ThreadLocal();
    final GiniStatistic.Split split_;
    final Data data_;
    final int depth_;
    
    FJGiniBuild(GiniStatistic.Split split, Data data, int depth) {
      this.split_ = split;
      this.data_ = data;
      this.depth_ = depth;
    }
    
    @Override public INode compute() {
      // first get the statistics
      GiniStatistic left = leftStat_.get();
      if (left == null) {
        left = new GiniStatistic(data_);
        leftStat_.set(left);
      } else {
        left.reset(data_);
      }
      GiniStatistic right = rightStat_.get();
      if (right == null) {
        right = new GiniStatistic(data_);
        rightStat_.set(right);
      } else {
        right.reset(data_);
      }
      // create the data, node and filter the data 
      Data[] res = new Data[2];
      GiniNode nd = new GiniNode(split_.column, split_.split,data_.data_);
      data_.filter(nd.column, nd.split,res,left,right);
      // get the splits
      GiniStatistic.Split ls = left.split();
      GiniStatistic.Split rs = right.split();
      // create leaf nodes if any
      if (ls.isLeafNode())
        nd.l_ = new LeafNode(ls.split);
      if (rs.isLeafNode())
        nd.r_ = new LeafNode(ls.split);
      // calculate the missing subnodes as new FJ tasks, join if necessary
      if ((nd.l_ == null) && (nd.r_ == null)) {
        ForkJoinTask<INode> fj0 = new FJGiniBuild(ls,res[0],depth_+1).fork();
        nd.r_ = new FJGiniBuild(rs,res[1],depth_+1).compute();
        nd.l_ = fj0.join();
      } else if (nd.l_ == null) {
        nd.l_ = new FJGiniBuild(ls,res[0],depth_+1).compute();
      } else if (nd.r_ == null) {
        nd.r_ = new FJGiniBuild(rs,res[1],depth_+1).compute();
      }
      // and return the node
      return nd;
    }
    
  }
  
  public static abstract class INode {
    abstract int classify(Row r);
    abstract int depth();       // Depth of deepest leaf
    abstract int leaves();      // Number of leaves
    abstract StringBuilder toString( StringBuilder sb, int len );

    public abstract void print(TreePrinter treePrinter) throws IOException;
    abstract void write( Stream bs );
    abstract int size( Stream bs ); // Size in serialized form
    static  INode read ( Stream bs, DataAdapter dapt ) {
      switch( bs.get1() ) {
      case '[':  return LeafNode.read(bs); // Leaf selector
      case '(':  return     Node.read(bs,dapt); // Node selector
      case 'G':  return GiniNode.read(bs,dapt); // Node selector
      default:
        throw new Error("Misformed serialized rf.Tree; expected to find an INode tag but found '"+(0xFF&bs._buf[bs._off-1])+"' instead");
      }
    }
  }

  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {
    final int class_;    // A category reported by the inner node
    LeafNode(int c) {
      assert 0 <= c && c < 100; // sanity check
      class_ = c;               // Class from 0 to _N-1
    }
    public int classify(Row r) { return class_; }
    int depth() { return 0; }
    int leaves(){ return 1; }
    public StringBuilder toString(StringBuilder sb, int n ) { return sb.append('[').append(class_).append(']'); }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( Stream bs ) {
      assert 0 <= class_ && class_ < 100;
      bs.set1('[');             // Leaf indicator
      bs.set1(class_);
    }
    static final int SIZE=2;
    int size( Stream bs ) { return SIZE; } // 2 bytes in serialized form
    static LeafNode read( Stream bs ) { return new LeafNode(bs.get1()&0xFF);  }
  }

  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  static class Node extends INode {
    INode l_, r_;
    final DataAdapter _dapt;
    final int column_;
    final double value_;
    int _depth, _leaves, _size;
    public Node(int column, double value, DataAdapter dapt) {
      column_= column;
      value_ = value;
      _dapt  = dapt;
    }
    public int classify(Row row) {
      return (row.getS(column_)<=value_ ? l_ : r_).classify(row);
    }
    public int depth() {
      if( _depth != 0 ) return _depth;
      return (_depth = Math.max(l_.depth(), r_.depth()) + 1);
    }
    public int leaves() {
      if( _leaves != 0 ) return _leaves;
      return (_leaves=l_.leaves() + r_.leaves());
    }

    // Computes the original split-value, as a float.  Returns a float to keep
    // the final size small for giant trees.
    private final C column() {
      return _dapt.c_[column_]; // Get the column in question
    }
    private final float split_value(C c) {
      short idx = (short)value_; // Convert split-point of the form X.5 to a (short)X
      double dlo = c._v2o[idx+0]; // Convert to the original values
      double dhi = (idx < c.sz_) ? c._v2o[idx+1] : dlo+1.0;
      double dmid = (dlo+dhi)/2.0; // Compute an original split-value
      float fmid = (float)dmid;
      assert (float)dlo < fmid && fmid < (float)dhi; // Assert that the float will properly split
      return fmid;
    }
    public StringBuilder toString( StringBuilder sb, int n ) {
      C c = column();           // Get the column in question
      sb.append(c.name_).append('<').append(Utils.p2d(split_value(c))).append(" (");
      if( sb.length() > n ) return sb;
      l_.toString(sb,n);
      if( sb.length() > n ) return sb;
      r_.toString(sb.append(','),n);
      if( sb.length() > n ) return sb;
      return sb.append(')');
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( Stream bs ) {
      bs.set1('(');             // Node indicator
      assert Short.MIN_VALUE <= column_ && column_ < Short.MAX_VALUE;
      bs.set2(column_);
      bs.set4f(split_value(column()));
      int skip = l_.size(bs); // Drop down the amount to skip over the left column
      if( skip <= 254 ) bs.set1(skip);
      else { bs.set1(0); bs.set3(skip); }
      l_.write(bs);
      r_.write(bs);
    }
    public int size( Stream bs ) {
      if( _size != 0 ) return _size;
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return _size=(1+2+4+(( l_.size(bs) <= 254 ) ? 1 : 4)+l_.size(bs)+r_.size(bs));
    }
    static Node read( Stream bs, DataAdapter dapt ) {
      int col = bs.get2();
      float f = bs.get4f();
      int idx = dapt.c_[col].o2v_.get(f); // Reverse float to short index; should not fail!!!
      Node n = new Node(col,((double)idx)+0.5,dapt);
      int skip = bs.get1();   // Skip (over left subtree) is either 1 byte or 4
      if( skip == 0 ) bs._off += 3; // Leading zero means there are 3 more bytes of skip
      n.l_ = INode.read(bs,dapt);
      n.r_ = INode.read(bs,dapt);
      return n;
    }
  }

  /** Gini classifier node. 
   * 
   */
  static class GiniNode extends INode {
    final DataAdapter _dapt;
    final int column;
    final int split;
    INode l_, r_;
    int _depth, _leaves, _size;

    public GiniNode(int column, int split, DataAdapter dapt) {
      _dapt = dapt;
      this.column = column;
      this.split = split;
    }
    @Override int classify(Row r) {
      return r.getColumnClass(column) <= split ? l_.classify(r) : r_.classify(r);
    }
    public int depth() {
      if( _depth != 0 ) return _depth;
      return (_depth = Math.max(l_.depth(), r_.depth()) + 1);
    }
    public int leaves() {
      if( _leaves != 0 ) return _leaves;
      return (_leaves=l_.leaves() + r_.leaves());
    }
    // Computes the original split-value, as a float.  Returns a float to keep
    // the final size small for giant trees.
    private final C column() {
      return _dapt.c_[column]; // Get the column in question
    }
    private final float split_value(C c) {
      short idx = (short)split;
      double dlo = c._v2o[idx+0]; // Convert to the original values
      double dhi = (idx < c.sz_) ? c._v2o[idx+1] : dlo+1.0;
      double dmid = (dlo+dhi)/2.0; // Compute an original split-value
      float fmid = (float)dmid;
      assert (float)dlo < fmid && fmid < (float)dhi; // Assert that the float will properly split
      return fmid;
    }
    public String toString() {
      return "G "+column +"<" + split + " ("+l_+","+r_+")";
    }
    public StringBuilder toString( StringBuilder sb, int n ) {
      sb.append("G ").append(column).append('<').append(split).append(" (");
      if( sb.length() > n ) return sb;
      sb = l_.toString(sb,n).append(',');
      if( sb.length() > n ) return sb;
      sb = r_.toString(sb,n).append(')');
      return sb;
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    
    void write( Stream bs ) {
      bs.set1('G');             // Node indicator
      assert Short.MIN_VALUE <= column && column < Short.MAX_VALUE;
      bs.set2(column);
      bs.set4f(split_value(column()));
      int skip = l_.size(bs); // Drop down the amount to skip over the left column
      if( skip <= 254 ) bs.set1(skip); else bs.set4(skip);
      l_.write(bs);
      r_.write(bs);
    }
    public int size( Stream bs ) {
      if( _size != 0 ) return _size;
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return _size=(1+2+4+(( l_.size(bs) <= 254 ) ? 1 : 4)+l_.size(bs)+r_.size(bs));
    }
    static GiniNode read( Stream bs, DataAdapter dapt ) {
      int col = bs.get2();
      float f = bs.get4f();
      int idx = dapt.c_[col].o2v_.get(f); // Reverse float to short index; should not fail!!!
      GiniNode n = new GiniNode(col,idx,dapt);
      int skip = bs.get1();     // Skip (over left subtree) is either 1 byte or 4
      if( skip == 0 ) bs._off += 3; // Leading zero means there are 3 more bytes of skip
      n.l_ = INode.read(bs,dapt);
      n.r_ = INode.read(bs,dapt);
      return n;
    }
  }
  
  
  public int classify(Row r) { return _tree.classify(r); }
  public String toString()   { return _tree.toString(); }

  // Write the Tree to a random Key homed here.
  public Key toKey() {
    Stream bs = new Stream();
    bs.set4(_data_id);
    _tree.write(bs);
    Key key = Key.make(UUID.randomUUID().toString(),(byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(key,new Value(key,bs.trim()));
    return key;
  }
  public static Tree fromKey( Key key, DataAdapter dapt ) {
    Stream bs = new Stream(DKV.get(key).get());
    Tree t = new Tree(bs.get4());
    t._tree = INode.read(bs,dapt);
    return t;
  }

  // Classify this serialized tree - withOUT inflating it to a full tree.
  // Use row 'row' in the dataset 'ary' (with pre-fetched bits 'databits' & 'rowsize')
  // Returns classes from 0 to N-1
  public static int classify( byte[] tbits, ValueArray ary, byte[] databits, int row, int rowsize ) {
    Stream ts = new Stream(tbits);
    int data_id = ts.get4();    // Skip tree-id
    while( ts.get1() != '[' ) { // While not a leaf indicator
      int o = ts._off-1;
      assert tbits[o] == '(' || tbits[o] == 'G';
      int col = ts.get2();      // Column number
      float fcmp = ts.get4f();  // Float to compare against
      float fdat = (float)ary.datad(databits,row,rowsize,col);
      int skip = (ts.get1()&0xFF);
      if( skip == 0 ) skip = ts.get3();
      if( fdat > fcmp )         // Picking right subtree?
        ts._off += skip;        // Skip left subtree
    }
    return ts.get1()&0xFF;      // Return the leaf's class
  }

  // Rather expensively, walk the entire tree counting leaves & max depth
  private static long d_l( Stream ts ) {
    if( ts.get1() == '[' ) return 1; // 1 leaf, 0 depth
    int o = ts._off-1;
    assert ts._buf[o] == '(' || ts._buf[o] == 'G';
    ts._off += 2+4;             // Skip col & float
    int skip = (ts.get1()&0xFF);
    if( skip == 0 ) skip = ts.get3();

    int roff = ts._off+skip;
    long dl1 = d_l(ts);          // Left side
    long d1  = dl1>>>32;
    long l1  = dl1&0xFFFFFFFFL;

    ts._off = roff;
    long dl2 = d_l(ts);          // Right side
    long d2  = dl2>>>32;
    long l2  = dl2&0xFFFFFFFFL;

    return ((Math.max(d1,d2)+1)<<32) | (l1+l2);
  }

  // Return (depth<<32)|(leaves), in 1 pass.
  public static long depth_leaves( byte[] tbits ) {
    Stream ts = new Stream(tbits);
    int data_id = ts.get4();    // Skip tree-id
    return d_l(ts);
  }
}
