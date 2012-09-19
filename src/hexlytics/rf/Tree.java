package hexlytics.rf;

import hexlytics.rf.Data.Row;

import java.io.IOException;
import java.util.UUID;

import jsr166y.CountedCompleter;
import jsr166y.RecursiveTask;
import water.*;

public class Tree extends CountedCompleter {
  static  boolean THREADED = false;  // multi-threaded ?

  ThreadLocal<Statistic>[] stats_;
  static public enum StatType { ENTROPY, GINI };
  final StatType _type;         // Flavor of split logic
  final Data _data;             // Data source
  final int _data_id; // Data-subset identifier (so trees built on this subset are not validated on it)
  final int _max_depth;         // Tree-depth cutoff
  final int _features;          // Number of features to use
  final double _min_error_rate; // Error rate below which a split isn't worth it
  INode _tree;                  // Root of decision tree
  int  _seed;

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate, StatType stat, int features, int seed) {
    _type = stat;
    _data = data;
    _data_id = data.data_._data_id;
    _max_depth = max_depth;
    _min_error_rate = min_error_rate;
    _features = features;
    _seed = seed;
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
    // Change this to a different amount of statistics, for each possible
    // subnode one, 2 for binary trees
    stats_ = new ThreadLocal[2];
    for (int i = 0; i < stats_.length; ++i) stats_[i] = new ThreadLocal<Statistic>();
  }

  private void freeStatistics() { stats_ = null; } // so that they can be GCed

  // TODO this has to change a lot, only a temp working version
  private Statistic getOrCreateStatistic(int index, Data data) {
    Statistic result = stats_[index].get();
    if( result==null ) {
      switch (_type) {
      case GINI:    result = new    GiniStatistic(data,_features, _seed);  break;
      case ENTROPY: result = new EntropyStatistic(data,_features, _seed);  break;
      default:      throw new Error("Unknown tree type to build the statistic. ");
      }
      stats_[index].set(result);
    }
    result.reset(data);
    return result;
  }

  // Actually build the tree
  public void compute() {
    createStatistics();
    // build the tree
    // first get the statistic so that it can be reused
    Statistic left = getOrCreateStatistic(0,_data);
    // calculate the split
    for( Row r : _data ) left.add(r);
    Statistic.Split spl = left.split(_data);
    _tree = spl.isLeafNode()
      ? new LeafNode(spl._split)
      : new FJBuild (spl,_data,0).compute();
    // report & bookkeeping
    StringBuilder sb = new StringBuilder();
    sb.append("Tree :").append(_data_id).append(" d=").append(_tree.depth());
    sb.append(" leaves=").append(_tree.leaves()).append("  ");
    sb = _tree.toString(sb,150);
    System.out.println(sb.toString());
    freeStatistics();
    tryComplete();
  }

  private class FJBuild extends RecursiveTask<INode> {
    final Statistic.Split split_;
    final Data data_;
    final int depth_;

    FJBuild(Statistic.Split split, Data data, int depth) {
      this.split_ = split;
      this.data_ = data;
      this.depth_ = depth;
    }

    @Override public INode compute() {
      Statistic left = getOrCreateStatistic(0,data_); // first get the statistics
      Statistic rite = getOrCreateStatistic(1,data_);
      Data[] res = new Data[2]; // create the data, node and filter the data
      SplitNode nd;
      if (split_.isExclusion()) {
        nd = new ExclusionNode(split_._column, split_._split, data_.data_);
        data_.filterExclude(nd._column, nd._split,res,left,rite);
      } else {
        nd = new SplitNode(split_._column, split_._split, data_.data_);
        data_.filter      (nd._column, nd._split,res,left,rite);
      }

      FJBuild fj0 = null, fj1 = null;
      Statistic.Split ls = left.split(data_); // get the splits
      Statistic.Split rs = rite.split(data_);
      if (ls.isLeafNode())  nd._l = new LeafNode(ls._split); // create leaf nodes if any
      else                    fj0 = new  FJBuild(ls,res[0],depth_+1);
      if (rs.isLeafNode())  nd._r = new LeafNode(rs._split);
      else                    fj1 = new  FJBuild(rs,res[1],depth_+1);

      // Recursively build the splits, in parallel
      if( fj0 != null &&        (fj1!=null && THREADED) ) fj0.fork();
      if( fj1 != null ) nd._r = fj1.compute();
      if( fj0 != null ) nd._l = (fj1!=null && THREADED) ? fj0.join() : fj0.compute();
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

  /** Gini classifier node. */
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
    protected final float split_value() { return  _dapt.unmap(_column,_split); }
    protected final C column() {
      return _dapt.c_[_column]; // Get the column in question
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    public String toString() {
      return "S "+_column +"<=" + _split + " ("+_l+","+_r+")";
    }
    public StringBuilder toString( StringBuilder sb, int n ) {
      C c = column();           // Get the column in question
      sb.append(c.name_).append("<=").append(Utils.p2d(split_value())).append(" (");
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

  /** Node that classifies one column category to the left and the others to the
   * right.
   */
  static class ExclusionNode extends SplitNode {
    public ExclusionNode(int column, int split, DataAdapter dapt) {
      super(column,split,dapt);
    }
    @Override int classify(Row r) {
      return r.getColumnClass(_column) == _split ? _l.classify(r) : _r.classify(r);
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    public String toString() {
      return "E "+_column +"==" + _split + " ("+_l+","+_r+")";
    }
    public StringBuilder toString( StringBuilder sb, int n ) {
      C c = column();           // Get the column in question
      sb.append(c.name_).append("==").append(_split).append(" (");
      if( sb.length() > n ) return sb;
      sb = _l.toString(sb,n).append(',');
      if( sb.length() > n ) return sb;
      sb = _r.toString(sb,n).append(')');
      return sb;
    }

    void write( Stream bs ) {
      bs.set1('E');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.set2(_column);
      bs.set4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 ) bs.set1(skip);
      else { bs.set1(0); bs.set3(skip); }
      _l.write(bs);
      _r.write(bs);
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
    ts.get4();    // Skip tree-id
    while( ts.get1() != '[' ) { // While not a leaf indicator
      int o = ts._off-1;
      byte b = tbits[o];
      assert tbits[o] == '(' || tbits[o] == 'S' || tbits[o] == 'E';
      int col = ts.get2();      // Column number
      float fcmp = ts.get4f();  // Float to compare against
      float fdat = (float)ary.datad(databits,row,rowsize,col);
      int skip = (ts.get1()&0xFF);
      if( skip == 0 ) skip = ts.get3();
      if (b == 'E') {
        if (fdat != fcmp)
          ts._off += skip;
      } else {
        if( fdat > fcmp )         // Picking right subtree?
          ts._off += skip;        // Skip left subtree
      }
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
      assert b == '(' || b == 'S' || b =='E';
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
      long result( ) {return ((long)_maxdepth<<32) | _leaves; }
    }.visit().result();
  }
}
