package hex.rf;

import hex.rf.Data.Row;

import java.io.IOException;
import java.util.UUID;

import jsr166y.CountedCompleter;
import jsr166y.RecursiveTask;
import water.DKV;
import water.H2O;
import water.Key;
import water.Stream;
import water.Value;
import water.ValueArray;

public class Tree extends CountedCompleter {
  static  boolean THREADED = false;  // multi-threaded ?

  static public enum StatType { ENTROPY, GINI };

  final StatType _type;         // Flavor of split logic
  final Data _data;             // Data source
  final int _data_id;           // Data-subset identifier (so trees built on this subset are not validated on it)
  final int _max_depth;         // Tree-depth cutoff
  /** Number of features to use */
  final int _features;
  final double _min_error_rate; // Error rate below which a split isn't worth it
  INode _tree;                  // Root of decision tree
  final int  _seed;             // Pseudo random seeds
  ThreadLocal<Statistic>[] _stats  = new ThreadLocal[2];

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate, StatType stat, int features, int seed) {
    _type = stat;
    _data = data;
    _data_id = data.dataId();
    _max_depth = max_depth-1;
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
    _seed = 0;
  }


  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  private Statistic getStatistic(int index, Data data, int seed) {
    Statistic result = _stats[index].get();
    if( result==null ) {
      result  = _type == StatType.GINI ?
          new GiniStatistic   (data,_features, _seed) :
          new EntropyStatistic(data,_features, _seed);
      _stats[index].set(result);
    }
    result.reset(data, seed);
    return result;
  }

  // Actually build the tree
  public void compute() {
    _stats[0] = new ThreadLocal<Statistic>();
    _stats[1] = new ThreadLocal<Statistic>();
    Statistic left = getStatistic(0,_data, _seed);
    // calculate the split
    for( Row r : _data ) left.add(r);
    Statistic.Split spl = left.split(_data,false);
    _tree = spl.isLeafNode()
      ? new LeafNode(spl._split)
      : new FJBuild (spl,_data,0, _seed + 1).compute();
    StringBuilder sb = new StringBuilder("Tree :"+_data_id+" d="+_tree.depth()+" leaves="+_tree.leaves()+"  ");
    Utils.pln(_tree.toString(sb,150).toString());
    _stats = null; // GC
    tryComplete();
  }

  private class FJBuild extends RecursiveTask<INode> {
    final Statistic.Split _split;
    final Data _data;
    final int _depth, _seed;

    FJBuild(Statistic.Split split, Data data, int depth, int seed) {
      _split = split;  _data = data; _depth = depth; _seed = seed;
    }

    @Override public INode compute() {
      Statistic left = getStatistic(0,_data, _seed + 10101); // first get the statistics
      Statistic rite = getStatistic(1,_data, _seed +  2020);
      Data[] res = new Data[2]; // create the data, node and filter the data
      int c = _split._column, s = _split._split;
      assert c != _data.classIdx();
      SplitNode nd = _split.isExclusion() ?
        new ExclusionNode(c, s, _data.colName(c), _data.unmap(c,s)) :
        new SplitNode    (c, s, _data.colName(c), _data.unmap(c, s) );
      _data.filter(nd,res,left,rite);

      FJBuild fj0 = null, fj1 = null;
      Statistic.Split ls = left.split(_data, _depth >= _max_depth); // get the splits
      Statistic.Split rs = rite.split(_data, _depth >= _max_depth);
      if (ls.isLeafNode())  nd._l = new LeafNode(ls._split); // create leaf nodes if any
      else                    fj0 = new  FJBuild(ls,res[0],_depth+1, _seed + 1);
      if (rs.isLeafNode())  nd._r = new LeafNode(rs._split);
      else                    fj1 = new  FJBuild(rs,res[1],_depth+1, _seed - 1);
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
    final int _class;    // A category reported by the inner node
    LeafNode(int c) {
      assert 0 <= c && c < 65534; // sanity check
      _class = c;               // Class from 0 to _N-1
    }
    @Override public int depth()  { return 0; }
    @Override public int leaves() { return 1; }
    public int classify(Row r) { return _class; }
    public StringBuilder toString(StringBuilder sb, int n ) { return sb.append('[').append(_class).append(']'); }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( Stream bs ) {
      assert 0 <= _class && _class < 100;
      bs.set1('[');             // Leaf indicator
      bs.set1(_class);
    }
    int size_impl( ) { return 2; } // 2 bytes in serialized form
  }


  /** Gini classifier node. */
  static class SplitNode extends INode {
    final int _column;
    final int _split;
    INode _l, _r;
    int _depth, _leaves, _size;
    String _name;
    float _originalSplit;

    public SplitNode(int column, int split, String columnName, float originalSplit) {
      _name = columnName;
      _column = column;
      _split = split;
      _originalSplit = originalSplit;
    }

    @Override int classify(Row r) {
      return r.getEncodedColumnValue(_column) <= _split ? _l.classify(r) : _r.classify(r);
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
    protected final float split_value() { return _originalSplit; }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    public String toString() {
      return "S "+_column +"<=" + _split + " ("+_l+","+_r+")";
    }
    public StringBuilder toString( StringBuilder sb, int n ) {
      sb.append(_name).append("<=").append(Utils.p2d(split_value())).append(" (");
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
    public boolean isIn(Row row) {  return row.getEncodedColumnValue(_column) <= _split; }
  }

  /** Node that classifies one column category to the left and the others to the
   * right.
   */
  static class ExclusionNode extends SplitNode {
    public ExclusionNode(int column, int split, String colName, float origSplit) {
      super(column,split,colName,origSplit);
    }
    @Override int classify(Row r) {
      return r.getEncodedColumnValue(_column) == _split ? _l.classify(r) : _r.classify(r);
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    public String toString() {
      return "E "+_column +"==" + _split + " ("+_l+","+_r+")";
    }
    public StringBuilder toString( StringBuilder sb, int n ) {
      sb.append(_name).append("==").append(_split).append(" (");
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
    public boolean isIn(Row row) {  return row.getEncodedColumnValue(_column) == _split; }
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
  public static short classify( byte[] tbits, ValueArray ary, byte[] databits, int row, int rowsize ) {
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
    return (short) ( ts.get1()&0xFF );      // Return the leaf's class
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
