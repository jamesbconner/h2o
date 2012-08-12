/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.Tree;
import hexlytics.data.Data;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import water.*;

/** This is the distributed builder of the random forest that works in hexbase.
 * 
 *
 *
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
  @Override
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

  @Override
  public void onBuilderTerminated() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onAggregatorChange() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onTreeValidated(Tree tree, int rows,int[] badRows, int[] badVotes) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onValidatorTerminated() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void report(String what) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  // DRemoteTask implementation ------------------------------------------------
  
  @Override
  public void map(Key key) {
    Data data = null;
    // start the validator
    // TODO
    
    // start the builder in our thread
    builder_ = new TreeBuilder(data,this,100);
    builder_.run();
    
    
  }

  @Override
  public void reduce(RemoteTask drt) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected int wire_len() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected int write(byte[] buf, int off) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected void write(DataOutputStream dos) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected void read(byte[] buf, int off) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected void read(DataInputStream dis) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
