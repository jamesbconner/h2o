package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Statistic.Split;
import java.io.*;
import java.util.UUID;
import jsr166y.*;
import water.*;

public class Tree extends CountedCompleter {
  final Data _data;
  final int _max_depth;
  final double _min_error_rate;
  INode tree_;

  // Constructor used to define the specs when building the tree from the top
  public Tree( Data data, int max_depth, double min_error_rate ) {
    _data = data;
    _max_depth = max_depth;
    _min_error_rate = min_error_rate;
  }
  // Constructor used to inhaling/de-serializing a pre-built tree.
  public Tree( ) {
    _data = null;
    _max_depth = 0;
    _min_error_rate = -1.0;
  }

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  // Actually build the tree
  public void compute() {
    // All rows in the top-level split
    Statistic s = new Statistic(_data,null);
    for (Row r : _data) s.add(r);
    tree_ = new FJBuild(s,_data,0).compute();
    //    ... next up: send tree to a distributed validator; record validation counts & confusion matrix per-tree; record global counts; puke incremental results to 1 place
    tryComplete();
  }

  private class FJBuild extends RecursiveTask<INode> {
    final Statistic _s;         // All the rows that this split munged over
    final Data _data;           // The resulting 1/2-sized dataset from the above split
    final int _d;
    FJBuild( Statistic s, Data data, int depth ) { _s = s; _data = data; _d = depth; }
    public INode compute() {
      // terminate the branch prematurely
      if( _d >= _max_depth || _s.error() < _min_error_rate )
        return new LeafNode(_d,_s.classOf());
      if (_s.singleClass()) return new LeafNode(_d, _s.classOf());
      Split best = _s.best();
      if (best == null) return new LeafNode(_d, _s.classOf());
      Node nd = new Node(_d, best.column,best.value,_data.data_);
      Data[] res = new Data[2];
      Statistic[] stats = new Statistic[] { new Statistic(_data,_s), new Statistic(_data,_s)};
      _data.filter(best,res,stats);
      ForkJoinTask<INode> fj0 = new FJBuild(stats[0],res[0],_d+1).fork();
      nd.r_ =                   new FJBuild(stats[1],res[1],_d+1).compute();
      nd.l_ = fj0.join();
      return nd;
    }
  }

  public static abstract class INode {
    public final int _depth;    // Depth in tree
    protected INode(int depth) { _depth = depth; }
    abstract int classify(Row r);
    public int depth()         { return 0; }
    public int leaves()        { return 1; }

    public abstract void print(TreePrinter treePrinter) throws IOException;
    abstract void write( DataOutputStream dos ) throws IOException;
    static INode read( DataInputStream dis, int depth ) throws IOException {
      int b = dis.readByte();
      switch( b ) {
      case '[':  return LeafNode.read(dis,depth); // Leaf selector
      case '(':  return     Node.read(dis,depth); // Node selector
      default:
        throw new Error("Misformed serialized rf.Tree; expected to find an INode tag but found '"+(char)b+"' instead");
      }
    }
  }

  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {
    final int class_;    // A category reported by the inner node
    LeafNode(int depth,int c) {
      super(depth);
      class_ = c;
    }
    public int classify(Row r) { return class_; }
    public String toString()   { return "["+class_+"]"; }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( DataOutputStream dos ) throws IOException {
      assert Short.MIN_VALUE <= class_ && class_ < Short.MAX_VALUE;
      dos.writeByte('[');       // Leaf indicator
      dos.writeShort(class_);
    }
    static LeafNode read( DataInputStream dis, int depth ) throws IOException {
      return new LeafNode(depth,dis.readShort());
    }
  }

  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  static class Node extends INode {
    final DataAdapter _dapt;
    final int column_;
    final double value_;
    INode l_, r_;
    public Node(int depth,int column, double value, DataAdapter dapt) {
      super(depth);
      column_= column;
      value_ = value;
      _dapt  = dapt;
    }
    public int classify(Row row) {
      return (row.getS(column_)<=value_ ? l_ : r_).classify(row);
    }

    public int depth()        { return Math.max(l_.depth(), r_.depth()) + 1; }
    public int leaves()       { return l_.leaves() + r_.leaves(); }
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
    public String toString() {
      C c = column();           // Get the column in question
      return c.name_ +"<" + Utils.p2d(split_value(c)) + " ("+l_+","+r_+")";
    }
    public void print(TreePrinter p) throws IOException { p.printNode(this); }
    void write( DataOutputStream dos ) throws IOException {
      dos.writeByte('(');       // Node indicator
      assert Short.MIN_VALUE <= column_ && column_ < Short.MAX_VALUE;
      dos.writeShort(column_);
      //dos.writeFloat(split_value(column()));
      dos.writeShort((short)value_);// Pass the short index instead of the actual value
      l_.write(dos);
      r_.write(dos);
    }
    static Node read( DataInputStream dis, int depth ) throws IOException {
      int col = dis.readShort();
      //float f = dis.readFloat();
      int idx = dis.readShort(); // Read the short index instead of the actual value
      Node n = new Node(depth,col,((double)idx)+0.5,null);
      n.l_ = INode.read(dis,depth+1);
      n.r_ = INode.read(dis,depth+1);
      return n;
    }
   }

  public int classify(Row r) { return tree_.classify(r); }
  public String toString()   { return tree_.toString(); }

  // Write the Tree to a random Key homed here.
  public Key toKey() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      tree_.write(new DataOutputStream(bos));
    } catch( IOException e ) { throw new Error(e); }
    Key key = Key.make(UUID.randomUUID().toString(),(byte)1,Key.DFJ_INTERNAL_USER, H2O.SELF);
    DKV.put(key,new Value(key,bos.toByteArray()));
    return key;
  }
  public static Tree fromKey( Key key ) {
    byte[] bits = DKV.get(key).get();
    Tree t = new Tree();
    try {
      t.tree_ = INode.read(new DataInputStream(new ByteArrayInputStream(bits)),0);
    } catch( IOException e ) { throw new Error(e); }
    return t;
  }

//  /** Computes the tree using the gini statistic.
//   *
//   */
//  final INode computeGini(int depth, Data d, GiniStatistic.Split split, GiniJob[] jobs,GiniStatistic[] stats) {
//    // reset the statistics
//    stats[0].reset(d);
//    stats[1].reset(d);
//    // create the node and filter the data
//    Data[] res = new Data[2];
//    GiniNode nd = null;
//    switch (GiniStatistic.type) {
//      case split:
//        nd = new GiniNode(depth, split.column, split.split);
//        d.filter(nd.column,nd.split,res,stats);
//        break;
//      case exclusion:
//        nd = new ExclusionGiniNode(depth,split.column, split.split);
//        d.filterExclusion(nd.column,nd.split,res,stats);
//        break;
//    }
//    // filter the data to the new statistics
//    GiniStatistic.Split ls = stats[0].split();
//    GiniStatistic.Split rs = stats[1].split();
//    if (ls.isLeafNode())
//      nd.l_ = new LeafNode(depth+1,ls.split);
//    else
//      jobs[0] = new GiniJob(this,nd,0,res[0],ls);
//    if (rs.isLeafNode())
//      nd.r_ = new LeafNode(depth+1,rs.split);
//    else
//      jobs[1] = new GiniJob(this,nd,1,res[1],rs);
//    return nd;
//  }
//
//  static class GiniNode extends INode {
//    final int column;
//    final int split;
//    INode l_, r_;
//
//    @Override int classify(Row r) {
//      return r.getColumnClass(column) <= split ? l_.classify(r) : r_.classify(r);
//    }
//    
//    public GiniNode(int depth, int column, int split) {
//      super(depth);
//      this.column = column;
//      this.split = split;
//    }
//
//    public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
//    public int depth()        { return Math.max(l_.depth(), r_.depth()) + 1; }
//    public int leaves()       { return l_.leaves() + r_.leaves(); }
//    public String toString() {
//      C c = RFGiniTask.data().data_.c_[column];
//      return "G "+c.name_ +"<" + split + " ("+l_+","+r_+")";
//    }
//    public void print(TreePrinter p) throws IOException { p.printNode(this); }
//    void write( DataOutputStream dos ) throws IOException { throw new Error("unimplemented"); }
//    static Node read( DataInputStream dis, int depth ) { throw new Error("unimplemented"); }
//  }
//  
//  static class ExclusionGiniNode extends GiniNode {
//
//    @Override int classify(Row r) {
//      return r.getColumnClass(column) == split ? l_.classify(r) : r_.classify(r);
//    }
//    
//    public ExclusionGiniNode(int depth, int column, int split) {
//      super(depth,column,split);
//    }
//
//    public String toString() {
//      C c = RFGiniTask.data().data_.c_[column];
//      return c.name_ +"==" + split + " ("+l_+","+r_+")";
//    }
//
//    public void print(TreePrinter p) throws IOException { p.printNode(this); }
//    void write( DataOutputStream dos ) throws IOException { throw new Error("unimplemented"); }
//    static Node read( DataInputStream dis, int depth ) { throw new Error("unimplemented"); }
//    
//  }
}
