package hexlytics;

import java.io.Serializable;

import hexlytics.Statistic.Split;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;


/**
 * @author peta
 */
public class TreeWithSerialization implements Serializable {  
  private static final long serialVersionUID = 7669063054915148060L;
  INode tree_ = null;
  long time_ = 0;
  private static String statistic_ = "Numeric"; // Default choice
  int serializedSize_ = 0;
  
  public TreeWithSerialization() {}
  
  /** Creates the tree from serialized data. */
  public TreeWithSerialization(byte[] from, int offset) {
    tree_ = deserializeNode(from, offset+4).result;
  }
  
  public final TreeWithSerialization compute(Data data) { 
    long t = System.currentTimeMillis(); 
    tree_ = compute_(data);
    time_ = System.currentTimeMillis()-t;
    return this;
  }
  
  /** for given data creates a node and returns it. */
  protected final INode compute_(Data d) {
    int classOf = -1;
    for(Row r : d) 
       if (classOf==-1)
         classOf = r.classOf(); 
       else
         if (classOf != r.classOf()) {
           classOf = -1;
           break;
         }
    if (classOf!=-1) { 
      serializedSize_ += LeafNode.SERIALIZED_SIZE; 
      return new LeafNode(classOf);
    } else {
      Statistic s = Statistic.make(statistic_, d);  
      Split best = s.best();
      if (best == null) {
        serializedSize_ += LeafNode.SERIALIZED_SIZE; 
        return new LeafNode(s.classOf());
        //n.set(direction, new LeafNode(s.classOf()));            
      } else {
        Node nd = new Node(best.column,best.value);
        Data[] res = new Data[2];
        d.filter(best,res);
        nd.set(0,compute_(res[0]));
        nd.set(1,compute_(res[1]));
        serializedSize_ += Node.SERIALIZED_SIZE; 
        return nd;
      }
    }
  }
  
  public INode tree() { return tree_; }
  
  public static abstract class INode  implements Serializable {    
    private static final long serialVersionUID = 4707665968083310297L;
    int navigate(double[]_) { return -1; }
    void set(int direction, INode n) { throw new Error("Unsupported"); }
    abstract int classify(double[] v);
  }
 
  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {     
    private static final long serialVersionUID = -4781620729751890945L;
    /** Type identifier of the node in the serialization */
    public static final byte NODE_TYPE = 0;
    /** Size of the serialized node. byte type and int class. */
    public static final int SERIALIZED_SIZE = 5;
    int class_ = -1;    // A category reported by the inner node
    LeafNode(int c)                 { class_ = c; }
    public int classify(double[] v) { return class_; }
    public String toString()        { return "["+class_+"]"; }
  }

  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  static class Node extends INode {   
    private static final long serialVersionUID = -967861474179047605L;
    /** Type identifier of the node in the serialization */
    public static final byte NODE_TYPE = 1;
    /** Size of the serialized node. Byte tupe, int column and double value. */
    public static final int SERIALIZED_SIZE = 13;
    final int column_;
    final double value_;
    INode l_, r_;
    public Node(int column, double value) { column_=column; value_=value;  }
    public int navigate(double[] v) { return v[column_]<=value_?0:1; }
    public int classify(double[] v) { return navigate(v)==0? l_.classify(v) : r_.classify(v); }
    public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
    public String toString() { return column_ +"@" + Utils.p2d(value_) + " ("+l_+","+r_+")"; } 
  }
  
  public int classify(Row r) {
    return tree_.classify(r.v);
  } 

  
  /** Returns the size required for the tree to serialize. In bytes. */
  public int serializedSize() {
    return 4 + serializedSize_; // 4 goes for the length
  }
  
  /** Serializes the tree to the given byte array at specified offset. */
  public void serialize(byte[] to, int offset) {
    offset += Utils.set4(to,offset,serializedSize_);
    if (serializedSize_!=0)
      serializeNode(tree_,to,offset);
  }
  
  /** Serializes the given node recursively to the given byte array. */
  private int serializeNode(INode node, byte[] to, int offset) {
    //System.out.println(offset);
    if (node instanceof LeafNode) {
      to[offset++] = LeafNode.NODE_TYPE;
      offset += Utils.set4(to,offset,((LeafNode)node).class_);
    } else {
      to[offset++] = Node.NODE_TYPE;
      offset += Utils.set4(to,offset,((Node)node).column_);
      offset += Utils.set8d(to,offset,((Node)node).value_);
      offset = serializeNode(((Node)node).l_,to,offset);
      offset = serializeNode(((Node)node).r_,to,offset);
    }
    return offset;
  }
  
  private class DeserializationResult {
    public final INode result;
    public final int offset;
    DeserializationResult(INode result, int offset) {
      this.result = result;
      this.offset = offset;
    }
  }
  
  /** Deserializes the nodes and returns them. */
  private DeserializationResult deserializeNode(byte[] from, int offset) {
    if (from[offset]==LeafNode.NODE_TYPE) {
      offset += 1;
      serializedSize_ += LeafNode.SERIALIZED_SIZE;
      return new DeserializationResult(new LeafNode(Utils.get4(from,offset)),offset+4);      
    } if (from[offset]==Node.NODE_TYPE) {
      offset += 1;
      serializedSize_ += Node.SERIALIZED_SIZE;
      int column = Utils.get4(from,offset);
      offset += 4;
      double value = Utils.get8d(from,offset);
      offset += 8;
      Node n = new Node(column,value);
      DeserializationResult r = deserializeNode(from,offset);
      n.set(0,r.result);
      r = deserializeNode(from,r.offset);
      n.set(1,r.result);
      return new DeserializationResult(n, r.offset);
    } else {
      throw new Error("Unrecognized node type "+from[offset]+" in deserialized tree");
    }
  }
  
  public String toString() { return tree_.toString(); } 
}


