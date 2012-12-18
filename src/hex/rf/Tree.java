package hex.rf;

import hex.rf.Data.Row;

import java.io.IOException;
import java.util.UUID;

import jsr166y.CountedCompleter;
import jsr166y.RecursiveTask;
import water.*;

public class Tree extends CountedCompleter {
  static public enum StatType { ENTROPY, GINI };

  final StatType _type;         // Flavor of split logic
  final Data _data;             // Data source
  final int _data_id;           // Data-subset identifier (so trees built on this subset are not validated on it)
  final int _max_depth;         // Tree-depth cutoff
  final int _features;          // Number of features to check at each splitting (~ split features)
  final double _min_error_rate; // Error rate below which a split isn't worth it
  INode _tree;                  // Root of decision tree
  ThreadLocal<Statistic>[] _stats  = new ThreadLocal[2];
  final Key _treesKey;
  final Key _modelKey;
  final int _alltrees;          // Number of trees expected to build a complete model
  final long _seed;             // Pseudo random seed: used to playback sampling
  final int _numrows;           // Used to playback sampling
  final float _sample;          // Sample rate
  transient Timer _timer;
  int[] _ignoreColumns;         // columns ignored by the tree
  boolean _stratify;
  int [] _strata;

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate, StatType stat, int features, long seed, Key treesKey, Key modelKey, int treeId, int alltrees, float sample, int rowsize, int[] ignoreColumns, boolean stratify, int [] strata) {
    _type = stat;
    _data = data;
    _data_id = treeId; //data.dataId();
    _max_depth = max_depth-1;
    _min_error_rate = min_error_rate;
    _features = features;
    _treesKey = treesKey;
    _modelKey = modelKey;
    _alltrees = alltrees;
    _seed = seed;
    _sample = sample;
    _numrows = rowsize;
    _ignoreColumns = ignoreColumns;
    assert sample <= 1.0f;
    _timer = new Timer();
    _stratify = stratify;
    _strata = strata;
  }

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  private Statistic getStatistic(int index, Data data, long seed) {
    Statistic result = _stats[index].get();
    if( result==null ) {
      result  = _type == StatType.GINI ?
          new GiniStatistic   (data,_features, _seed) :
          new EntropyStatistic(data,_features, _seed);
      _stats[index].set(result);
    }
    result.forget_features();   // All new features
    result.reset(data, seed);
    return result;
  }

  // Actually build the tree
  public void compute() {
    _stats[0] = new ThreadLocal<Statistic>();
    _stats[1] = new ThreadLocal<Statistic>();
    Timer t_sample = new Timer();
    Data d = (true && _stratify)?_data.sample(_strata,_seed):_data.sample(_sample,_seed,_numrows);
    Utils.pln("[RF] Tree " + (_data_id+1)+ " sample done in "+ t_sample + ", seed = " + _seed);
    Statistic left = getStatistic(0, d, _seed);
    // calculate the split
    for( Row r : d ) left.addQ(r);
    left.applyClassWeights();   // Weight the distributions
    Statistic.Split spl = left.split(d, false);
    _tree = spl.isLeafNode()
      ? new LeafNode(spl._split)
      : new FJBuild (spl, d, 0, _seed + (1L)<<16).compute();
    StringBuilder sb = new StringBuilder("Tree : " +(_data_id+1)+" d="+_tree.depth()+" leaves="+_tree.leaves()+"  ");
    Utils.pln(_tree.toString(sb,200).toString());
    _stats = null; // GC
    new AppendKey(toKey()).invoke(_treesKey); // Atomic-append to the list of trees
    // Atomically improve the Model as well
    Model m = new Model(_modelKey, _treesKey, _data.columns(), _data.classes(), _sample, _data._data._ary._key,_ignoreColumns, _features);
    AtomicModel am = new AtomicModel(_alltrees, m);
    am.invoke(_modelKey);

    Utils.pln("[RF] Tree "+(_data_id+1) + " done in "+ _timer);
    tryComplete();
  }

  static class AtomicModel extends TAtomic<Model> {
    Model _model;
    private int _ntree;

    public AtomicModel() { }
    public AtomicModel( int tree, Model m ) {
      _ntree = tree;
      _model = m;
    }

    @Override public Model alloc() { return new Model(); }

    @Override
    public Model atomic(Model old) {
      // Abort the XTN with no change
      if( old != null && old.size() >= _model.size() ) return null;
      return _model;
    }

    @Override
    public void onSuccess() {
      if( _ntree == _model.size() && _model._treesKey != null)
        UKV.remove(_model._treesKey);
      _model._treesKey = null;
    }
  }

  private class FJBuild extends RecursiveTask<INode> {
    final Statistic.Split _split;
    final Data _data;
    final int _depth;
    final long _seed;

    FJBuild(Statistic.Split split, Data data, int depth, long seed) {
      _split = split;  _data = data; _depth = depth; _seed = seed;
    }

    @Override public INode compute() {
      Statistic left = getStatistic(0,_data, _seed + (10101L<<16)); // first get the statistics
      Statistic rite = getStatistic(1,_data, _seed + ( 2020L<<16));
      Data[] res = new Data[2]; // create the data, node and filter the data
      int c = _split._column, s = _split._split;
      assert c != _data.classIdx();
      SplitNode nd = _split.isExclusion() ?
        new ExclusionNode(c, s, _data.colName(c), _data.unmap(c,s)) :
        new SplitNode    (c, s, _data.colName(c), _data.unmap(c, s) );
      _data.filter(nd,res,left,rite);

      FJBuild fj0 = null, fj1 = null;
      Statistic.Split ls = left.split(res[0], _depth >= _max_depth); // get the splits
      Statistic.Split rs = rite.split(res[1], _depth >= _max_depth);
      if (ls.isLeafNode())  nd._l = new LeafNode(ls._split); // create leaf nodes if any
      else                    fj0 = new  FJBuild(ls,res[0],_depth+1, _seed + (1L<<16));
      if (rs.isLeafNode())  nd._r = new LeafNode(rs._split);
      else                    fj1 = new  FJBuild(rs,res[1],_depth+1, _seed - (1L<<16));
      // Recursively build the splits, in parallel
      if( fj0 != null &&        (fj1!=null ) ) fj0.fork();
      if( fj1 != null ) nd._r = fj1.compute();
      if( fj0 != null ) nd._l = (fj1!=null ) ? fj0.join() : fj0.compute();
      return nd;
    }
  }

  public static abstract class INode {
    abstract int classify(Row r);
    abstract int depth();       // Depth of deepest leaf
    abstract int leaves();      // Number of leaves
    abstract StringBuilder toString( StringBuilder sb, int len );

    public abstract void print(TreePrinter treePrinter) throws IOException;
    abstract void write( AutoBuffer bs );
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
    @Override public int classify(Row r) { return _class; }
    @Override public StringBuilder toString(StringBuilder sb, int n ) { return sb.append('[').append(_class).append(']'); }
    @Override public void print(TreePrinter p) throws IOException { p.printNode(this); }
    @Override void write( AutoBuffer bs ) {
      assert 0 <= _class && _class < 100;
      bs.put1('[');             // Leaf indicator
      bs.put1(_class);
    }
    @Override int size_impl( ) { return 2; } // 2 bytes in serialized form
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
    @Override public int depth() {
      if( _depth != 0 ) return _depth;
      return (_depth = Math.max(_l.depth(), _r.depth()) + 1);
    }
    @Override public int leaves() {
      if( _leaves != 0 ) return _leaves;
      return (_leaves=_l.leaves() + _r.leaves());
    }
    // Computes the original split-value, as a float.  Returns a float to keep
    // the final size small for giant trees.
    protected final float split_value() { return _originalSplit; }
    @Override public void print(TreePrinter p) throws IOException { p.printNode(this); }
    @Override public String toString() {
      return "S "+_column +"<=" + _split + " ("+_l+","+_r+")";
    }
    @Override public StringBuilder toString( StringBuilder sb, int n ) {
      sb.append(_name).append("<=").append(Utils.p2d(split_value())).append(" (");
      if( sb.length() > n ) return sb;
      sb = _l.toString(sb,n).append(',');
      if( sb.length() > n ) return sb;
      sb = _r.toString(sb,n).append(')');
      return sb;
    }

    @Override void write( AutoBuffer bs ) {
      bs.put1('S');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.put2((short) _column);
      bs.put4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 )  bs.put1(skip);
      else { bs.put1(0); bs.put3(skip); }
      _l.write(bs);
      _r.write(bs);
    }
    @Override public int size_impl( ) {
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
    @Override public void print(TreePrinter p) throws IOException { p.printNode(this); }
    @Override public String toString() {
      return "E "+_column +"==" + _split + " ("+_l+","+_r+")";
    }
    @Override public StringBuilder toString( StringBuilder sb, int n ) {
      sb.append(_name).append("==").append(_split).append(" (");
      if( sb.length() > n ) return sb;
      sb = _l.toString(sb,n).append(',');
      if( sb.length() > n ) return sb;
      sb = _r.toString(sb,n).append(')');
      return sb;
    }

    @Override void write( AutoBuffer bs ) {
      bs.put1('E');             // Node indicator
      assert Short.MIN_VALUE <= _column && _column < Short.MAX_VALUE;
      bs.put2((short)_column);
      bs.put4f(split_value());
      int skip = _l.size(); // Drop down the amount to skip over the left column
      if( skip <= 254 )  bs.put1(skip);
      else { bs.put1(0); bs.put3(skip); }
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
    AutoBuffer bs = new AutoBuffer();
    bs.put4(_data_id);
    bs.put8(_seed);
    _tree.write(bs);
    Key key = Key.make(UUID.randomUUID().toString(),(byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(key,new Value(key, bs.buf()));
    return key;
  }

  /** Classify this serialized tree - withOUT inflating it to a full tree.
     Use row 'row' in the dataset 'ary' (with pre-fetched bits 'databits' & 'rowsize')
     Returns classes from 0 to N-1*/
  public static short classify( AutoBuffer ts, ValueArray ary, AutoBuffer databits, int row, int rowsize, short badData ) {
    ts.get4();    // Skip tree-id
    ts.get8();    // Skip seed

    while( ts.get1() != '[' ) { // While not a leaf indicator
      int o = ts.position() - 1;
      byte b = (byte) ts.get1(o);
      assert b == '(' || b == 'S' || b == 'E';
      int col = ts.get2();      // Column number
      float fcmp = ts.get4f();  // Float to compare against
      if( ary.isNA(databits, row, col) ) return badData;
      float fdat = (float)ary.datad(databits, row, col);
      int skip = (ts.get1()&0xFF);
      if( skip == 0 ) skip = ts.get3();
      if (b == 'E') {
        if (fdat != fcmp)
          ts.position(ts.position() + skip);
      } else {
        // Picking right subtree? then skip left subtree
        if( fdat > fcmp ) ts.position(ts.position() + skip);
      }
    }
    return (short) ( ts.get1()&0xFF );      // Return the leaf's class
  }

  public static int dataId( byte[] bits) { return UDP.get4(bits, 0); }
  public static long seed ( byte[] bits) { return UDP.get8(bits, 4); }

  // Abstract visitor class for serialized trees.
  public static abstract class TreeVisitor<T extends Exception> {
    TreeVisitor<T> leaf( int tclass          ) throws T { return this; }
    TreeVisitor<T>  pre( int col, float fcmp, int off0, int offl, int offr ) throws T { return this; }
    TreeVisitor<T>  mid( int col, float fcmp ) throws T { return this; }
    TreeVisitor<T> post( int col, float fcmp ) throws T { return this; }
    long  result( ) { return 0; }
    protected final AutoBuffer _ts;
    TreeVisitor( AutoBuffer tbits ) {
      _ts = tbits;
      _ts.get4();               // Skip tree ID
      _ts.get8();               // Skip seed
    }

    final TreeVisitor<T> visit() throws T {
      byte b = (byte) _ts.get1();
      if( b == '[' ) return leaf(_ts.get1()&0xFF);
      assert b == '(' || b == 'S' || b =='E';
      int off0 = _ts.position()-1;    // Offset to start of *this* node
      int col = _ts.get2();     // Column number
      float fcmp = _ts.get4f(); // Float to compare against
      int skip = (_ts.get1()&0xFF);
      if( skip == 0 ) skip = _ts.get3();
      int offl = _ts.position();      // Offset to start of *left* node
      int offr = _ts.position()+skip; // Offset to start of *right* node
      return pre(col,fcmp,off0,offl,offr).visit().mid(col,fcmp).visit().post(col,fcmp);
    }
  }

  // Return (depth<<32)|(leaves), in 1 pass.
  public static long depth_leaves( AutoBuffer tbits ) {
    return new TreeVisitor<RuntimeException>(tbits) {
      int _maxdepth, _depth, _leaves;
      TreeVisitor leaf(int tclass ) { _leaves++; if( _depth > _maxdepth ) _maxdepth = _depth; return this; }
      TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) { _depth++; return this; }
      TreeVisitor post(int col, float fcmp ) { _depth--; return this; }
      long result( ) {return ((long)_maxdepth<<32) | _leaves; }
    }.visit().result();
  }
}
