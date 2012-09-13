package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Statistic.Split;
import java.io.IOException;
import java.util.UUID;
import jsr166y.CountedCompleter;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveTask;
import water.*;

public class Tree extends CountedCompleter {

  static  boolean THREADED = false;  // multi-threaded ?

  
  ThreadLocal<BaseStatistic>[] stats_;
  static public enum StatType { ENTROPY, NEW_ENTROPY, GINI };
  final StatType _type;         // Flavor of split logic
  final Data _data;             // Data source
  final int _data_id; // Data-subset identifier (so trees built on this subset are not validated on it)
  final int _max_depth;         // Tree-depth cutoff
  final int _features;          // Number of features to use
  final double _min_error_rate; // Error rate below which a split isn't worth it
  INode _tree;                  // Root of decision tree

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate, StatType stat, int features ) {
    _type = stat;
    _data = data;
    _data_id = data.data_._data_id;
    _max_depth = max_depth;
    _min_error_rate = min_error_rate;
    _features = features;
  }
  // Constructor used to inhaling/de-serializing a pre-built tree.
  public Tree( int data_id ) {
    _type = StatType.ENTROPY;// Junk stat; not sensible except during building
    _data = null;
    _data_id = data_id;
    _max_depth = 0;
    _min_error_rate = -1.0;
    _features = 0;
  }

  /** Determines the error rate of a single tree on the local data only. */
  public double validate(Data data) {
    int errors = 0, total = 0;
    for (Row row: data) {
      total++;
      if (row.classOf() != classify(row)) errors++;
    }
    return ((double)errors)/total;
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


  // Actually build the tree
  public void compute() {
    createStatistics();
    switch( _type ) {
      case ENTROPY:
        computeNumeric();
        break;
      case GINI:   
      case NEW_ENTROPY:
        computeGini();
        break;
      default:
        throw new Error("Unrecognized statistic type");
    }
    StringBuilder sb = new StringBuilder();
    sb.append("Tree :").append(_data_id).append(" d=").append(_tree.depth());
    sb.append(" leaves=").append(_tree.leaves()).append("  ");
    sb = _tree.toString(sb,150);
    System.out.println(sb.toString());
    freeStatistics();
    tryComplete();
  }

  void computeNumeric() { // All rows in the top-level split
    Statistic s = new Statistic(_data,null,_features);
    for (Row r : _data) s.add(r);
    _tree = new FJEntropyBuild(s,_data,0).compute();
  }

  void computeGini() {
    // first get the statistic so that it can be reused
    BaseStatistic left = getOrCreateStatistic(0,_data);
    // calculate the split
    for (Row r : _data) left.add(r);
    BaseStatistic.Split spl = left.split();
    if (spl.isLeafNode())  _tree = new LeafNode(spl.split);
    else  _tree = new FJBuild(spl,_data,0).compute();
  }

  private class FJEntropyBuild extends RecursiveTask<INode> {


    Statistic _s;         // All the rows that this split munged over
    Data _data;           // The resulting 1/2-sized dataset from the above split
    final int _d;               // depth

    FJEntropyBuild( Statistic s, Data data, int depth ) { _s = s; _data = data; _d = depth; }
    public INode compute() {
      // terminate the branch prematurely
      if( (_d >= _max_depth) && (_max_depth!=-1) ) // FIXME...  || _s.error() < _min_error_rate )
        return new LeafNode(_s.classOf());
      if (_s.singleClass()) return new LeafNode(_s.classOf());
      Split best = _s.best();
      if (best == null) return new LeafNode(_s.classOf());
      Node nd = new Node(best.column,best.value,_data.data_);
      Data[] res = new Data[2];
      Statistic[] stats = new Statistic[] { new Statistic(_data,_s, _s._features), new Statistic(_data,_s, _s._features)};
      _data.filter(best,res,stats);
      _data = null; _s = null;
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
      SplitNode nd = new SplitNode(split_.column, split_.split, data_.data_);
      data_.filter(nd._column, nd._split,res,left,right);
      BaseStatistic.Split ls = left.split();      // get the splits
      BaseStatistic.Split rs = right.split();
      if (ls.isLeafNode())  nd._l = new LeafNode(ls.split);      // create leaf nodes if any
      if (rs.isLeafNode())  nd._r = new LeafNode(rs.split);
      if ((nd._l == null) && (nd._r == null)) {   // calculate the missing subnodes as new FJ tasks, join if necessary
        ForkJoinTask<INode> fj0 = null;              
        if (THREADED) {
          fj0 = new FJBuild(ls,res[0],depth_+1).fork();
        } else {
         nd._l = new FJBuild(ls,res[0],depth_+1).compute();
        }
        nd._r = new FJBuild(rs,res[1],depth_+1).compute();
        if (THREADED) 
          nd._l = fj0.join();
      } else if (nd._l == null)   nd._l = new FJBuild(ls,res[0],depth_+1).compute();
      else if (nd._r == null)     nd._r = new FJBuild(rs,res[1],depth_+1).compute();
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
    int _size;                  // Byte-size in serialized form
    final int size( ) { return _size==0 ? (_size=size_impl()) : _size;  }
    abstract int size_impl();
  }

  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {
    final int class_;    // A category reported by the inner node
    LeafNode(int c) {
      assert 0 <= c && c < 100; // sanity check
      class_ = c;               // Class from 0 to _N-1
    }
    @Override public int depth()  { return 0; }
    @Override public int leaves() { return 1; }
    public int classify(Row r) { return class_; }
    public StringBuilder toString(StringBuilder sb, int n ) { return sb.append('[').append(class_).append(']'); }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( Stream bs ) {
      assert 0 <= class_ && class_ < 100;
      bs.set1('[');             // Leaf indicator
      bs.set1(class_);
    }
    int size_impl( ) { return 2; } // 2 bytes in serialized form
  }

  // Inner node of the decision tree. Contains a list of subnodes and the
  // classifier to be used to decide which subtree to explore further.
  static class Node extends INode {
    INode _l, _r;
    final DataAdapter _dapt;
    final int _column;
    final float _value;
    int _depth, _leaves;
    public Node(int column, float value, DataAdapter dapt) {
      _column= column;
      _value = value;
      _dapt  = dapt;
    }
    public int classify(Row row) { return (row.getS(_column)<=_value ? _l : _r).classify(row);  }
    public int depth() {
      if( _depth != 0 ) return _depth;
      return (_depth = Math.max(_l.depth(), _r.depth()) + 1);
    }
    public int leaves() {
      if( _leaves != 0 ) return _leaves;
      return (_leaves=_l.leaves() + _r.leaves());
    }
    // Computes the original split-value, as a float.  Returns a float to keep
    // the final size small for giant trees.
    private final float split_value() { return  _dapt.unmap(_column, _value); } 
    private final C column() { return _dapt.c_[_column]; } // Get the column in question
    public StringBuilder toString( StringBuilder sb, int n ) {
      C c = column();           // Get the column in question
      sb.append(c.name_).append('<').append(Utils.p2d(split_value())).append(" (");
      if( sb.length() > n ) return sb;
      _l.toString(sb,n);
      if( sb.length() > n ) return sb;
      _r.toString(sb.append(','),n);
      if( sb.length() > n ) return sb;
      return sb.append(')');
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( Stream bs ) {
      bs.set1('(');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.set2(_column);
      bs.set4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 ) bs.set1(skip);
      else { bs.set1(0); bs.set3(skip); }
      _l.write(bs);
      _r.write(bs);
    }
    public int size_impl(  ) {
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return _size=(1+2+4+(( _l.size() <= 254 ) ? 1 : 4)+_l.size()+_r.size());
    }
  }

  /** Gini classifier node.
   */
  static class SplitNode extends INode {
    final DataAdapter _dapt;
    final int _column;
    final int _split;
    INode _l, _r;
    int _depth, _leaves, _size;

    public SplitNode(int column, int split, DataAdapter dapt) {
      _dapt = dapt;
      _column = column;
      _split = split;
    }

    @Override int classify(Row r) {
      return r.getColumnClass(_column) <= _split ? _l.classify(r) : _r.classify(r);
    }
    public int depth() {
      if( _depth != 0 ) return _depth;
      return (_depth = Math.max(_l.depth(), _r.depth()) + 1);
    }
    public int leaves() {
      if( _leaves != 0 ) return _leaves;
      return (_leaves=_l.leaves() + _r.leaves());
    }
    // Computes the original split-value, as a float.  Returns a float to keep
    // the final size small for giant trees.
    private final float split_value() { return  _dapt.unmap(_column,_split); } 
    private final C column() {
      return _dapt.c_[_column]; // Get the column in question
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    public String toString() {
      return "S "+_column +"<=" + _split + " ("+_l+","+_r+")";
    }
    public StringBuilder toString( StringBuilder sb, int n ) {
      C c = column();           // Get the column in question
      sb.append(c.name_).append('<').append(Utils.p2d(split_value())).append(" (");
      if( sb.length() > n ) return sb;
      sb = _l.toString(sb,n).append(',');
      if( sb.length() > n ) return sb;
      sb = _r.toString(sb,n).append(')');
      return sb;
    }
    
    void write( Stream bs ) {
      bs.set1('S');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.set2(_column);
      bs.set4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 ) bs.set1(skip);
      else { bs.set1(0); bs.set3(skip); }
      _l.write(bs);
      _r.write(bs);
    }
    public int size_impl( ) {
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return _size=(1+2+4+(( _l.size() <= 254 ) ? 1 : 4)+_l.size()+_r.size());
    }
  }
  public int classify(Row r) { return _tree.classify(r); }
  public String toString()   { return _tree.toString(); }
  public int leaves() { return _tree.leaves(); }
  public int depth() { return _tree.depth(); }

  // Write the Tree to a random Key homed here.
  public Key toKey() {
    Stream bs = new Stream();
    bs.set4(_data_id);
    _tree.write(bs);
    Key key = Key.make(UUID.randomUUID().toString(),(byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(key,new Value(key,bs.trim()));
    return key;
  }

  // Classify this serialized tree - withOUT inflating it to a full tree.
  // Use row 'row' in the dataset 'ary' (with pre-fetched bits 'databits' & 'rowsize')
  // Returns classes from 0 to N-1
  public static int classify( byte[] tbits, ValueArray ary, byte[] databits, int row, int rowsize ) {
    Stream ts = new Stream(tbits);
    int data_id = ts.get4();    // Skip tree-id
    while( ts.get1() != '[' ) { // While not a leaf indicator
      int o = ts._off-1;
      assert tbits[o] == '(' || tbits[o] == 'S';
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

  // Abstract visitor class for serialized trees.
  public static abstract class TreeVisitor<T extends Exception> {
    TreeVisitor<T> leaf( int tclass          ) throws T { return this; }
    TreeVisitor<T>  pre( int col, float fcmp, int off0, int offl, int offr ) throws T { return this; }
    TreeVisitor<T>  mid( int col, float fcmp ) throws T { return this; }
    TreeVisitor<T> post( int col, float fcmp ) throws T { return this; }
    long  result( ) { return 0; }
    protected final Stream _ts;
    TreeVisitor( byte[] tbits ) {
      _ts = new Stream(tbits);
      _ts.get4();               // Skip tree ID
    }
    final TreeVisitor<T> visit() throws T {
      byte b = _ts.get1();
      if( b == '[' ) return leaf(_ts.get1()&0xFF);
      assert b == '(' || b == 'S';
      int off0 = _ts._off-1;    // Offset to start of *this* node
      int col = _ts.get2();     // Column number
      float fcmp = _ts.get4f(); // Float to compare against
      int skip = (_ts.get1()&0xFF);
      if( skip == 0 ) skip = _ts.get3();
      int offl = _ts._off;      // Offset to start of *left* node
      int offr = _ts._off+skip; // Offset to start of *right* node
      return pre(col,fcmp,off0,offl,offr).visit().mid(col,fcmp).visit().post(col,fcmp);
    }
  }

  // Return (depth<<32)|(leaves), in 1 pass.
  public static long depth_leaves( byte[] tbits ) {
    return new TreeVisitor<RuntimeException>(tbits) {
      int _maxdepth, _depth, _leaves;
      TreeVisitor leaf(int tclass ) { _leaves++; if( _depth > _maxdepth ) _maxdepth = _depth; return this; }
      TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) { _depth++; return this; }
      TreeVisitor post(int col, float fcmp ) { _depth--; return this; }
      long result( ) {return ((long)_maxdepth<<32) | (long)_leaves; }
    }.visit().result();
  }
}
