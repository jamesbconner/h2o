package hexlytics.RFBuilder;

import hexlytics.Tree;
import hexlytics.data.Data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.DRemoteTask;
import water.Key;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

/** This is the distributed builder of the random forest that works in hexbase.
 * @author peta
 */
@SuppressWarnings("serial")
@RTSerializer(HexBaseBuilder.Serializer.class)
public class HexBaseBuilder extends DRemoteTask implements Director {
  public static class Serializer extends RemoteTaskSerializer<HexBaseBuilder> {
    @Override public int wire_len(HexBaseBuilder task) { throw new RuntimeException("TODO Auto-generated method stub"); }
    @Override public int write(HexBaseBuilder task, byte[] buf, int off) { throw new RuntimeException("TODO Auto-generated method stub"); }
    @Override public void write(HexBaseBuilder task, DataOutputStream dos) throws IOException { throw new RuntimeException("TODO Auto-generated method stub"); }
    @Override public HexBaseBuilder read(byte[] buf, int off) { throw new RuntimeException("TODO Auto-generated method stub"); }
    @Override public HexBaseBuilder read(DataInputStream dis) throws IOException { throw new RuntimeException("TODO Auto-generated method stub"); }
  }

  int _myNodeId;
  String _nodePrefix = null;
  int treeIndex_ = 0;

  TreeBuilder builder_;
  TreeValidator validator_;
  
  // Director implementation ---------------------------------------------------
  
  /** When a tree is ready, stores it to a special KV pair so that it is
   * visible to the validators. */
  public void onTreeBuilt(Tree tree) {
    int treeIndex;
    synchronized (this) {
      treeIndex = treeIndex_++;
    }
    Key key = Key.make(_nodePrefix + _myNodeId + "_" + treeIndex);
  if(true) new Error("Oops... in the process of moving to message");
//    Value val = new Value(key, tree.serializedSize());
    // tree.serialize(val.mem(), 0);
//    DKV.put(key, val); // publish the tree to the validators
  }

  public void report(String what) { throw new RuntimeException("TODO: implement"); }

  public String nodeName() { return Integer.toString(_myNodeId); }
  
  // DRemoteTask implementation ------------------------------------------------
  
  public void map(Key key) {
    Data data = null;
    // start the validator
    // TODO
    
    // start the builder in our thread
    builder_ = new TreeBuilder(data,this,100);
    builder_.run(); 
  }

  @Override
  public void error(long error) {
    throw new RuntimeException("TODO Auto-generated method stub");
  }
  
  @Override
  public void reduce(DRemoteTask drt) {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

}
