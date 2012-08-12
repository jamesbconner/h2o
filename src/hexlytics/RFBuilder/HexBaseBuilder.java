package hexlytics.RFBuilder;

import hexlytics.Tree;
import hexlytics.data.Data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.DKV;
import water.DRemoteTask;
import water.Key;
import water.RemoteTask;
import water.Value;

/** This is the distributed builder of the random forest that works in hexbase.
 * @author peta
 */
public class HexBaseBuilder extends DRemoteTask implements Director {

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
    Value val = new Value(key, tree.serializedSize());
    tree.serialize(val.mem(), 0);
    DKV.put(key, val); // publish the tree to the validators
  }

  UnsupportedOperationException uoe() { return new UnsupportedOperationException("Not supported yet."); }
 
  public void onBuilderTerminated() { throw uoe(); }
  public void onAggregatorChange() {throw uoe(); }
  public void onTreeValidated(Tree tree, int rows,int[] badRows, int[] badVotes) { throw uoe(); }  
  public void onValidatorTerminated() { throw uoe(); }
  public void report(String what) { throw uoe(); }

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

  public void reduce(RemoteTask drt) {throw uoe(); }
  protected int wire_len() { throw uoe(); }
  protected int write(byte[] buf, int off) { throw uoe(); }
  @Override
  protected void write(DataOutputStream dos) throws IOException { throw uoe(); }
  @Override
  protected void read(byte[] buf, int off) { throw uoe(); }
  @Override
  protected void read(DataInputStream dis) throws IOException { throw uoe(); }

}
