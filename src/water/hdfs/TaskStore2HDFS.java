package water.hdfs;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import water.*;

/**
 * Distributed task to store key on HDFS.
 *
 * If it is a simple value, task is sent to the home of the value and the value
 * is simply stored on hdfs.  For arraylets, chunks are stored in order by
 * their home nodes.  Each node continues storing chunks until the next to be
 * stored has different home in which case the task is passed to the home node
 * of that chunk.
 *
 * @author tomasnykodym, cliffc
 *
 */
public class TaskStore2HDFS extends DTask {
  Key _arykey;
  long _indexFrom;

  public static Key store2Hdfs(Key srcKey) {
    assert srcKey._kb[0] != Key.ARRAYLET_CHUNK;
    Value v = DKV.get(srcKey);
    assert v!= null;            // Persisting junk keys?
    String p = getPathFromValue(v);
    Key result = PersistHdfs.getKeyForPathString(p);
    System.out.println("store2Hdfs path="+p+" result="+result);
    if( v._isArray == 0 ) {     // Simple chunk?
      boolean res = PersistHdfs.storeChunk(v.get(), p);
      assert res;
      throw H2O.unimpl();
      //PersistHdfs.refreshHDFSKeys();
      //return result;
    }

    // For ValueArrays, make the .hex header
    boolean res = PersistHdfs.createHexHeader(v, p);
    assert res;

    // The task managing which chunks to write next,
    // store in a known key
    TaskStore2HDFS ts = new TaskStore2HDFS(srcKey);
    Key selfKey = ts.selfKey();
    UKV.put(selfKey,ts);

    // Then start writing chunks in-order with the zero chunk
    H2ONode chk0_home = ValueArray.get_key(0,srcKey).home_node();
    RPC.call(ts.nextHome(),ts);

    // Watch the progress key until it gets removed
    while( DKV.get(selfKey) != null )
      try { Thread.sleep(100); } catch( InterruptedException e ) { }

    throw H2O.unimpl();
    //PersistHdfs.refreshHDFSKeys();
    //return result;
  }

  public TaskStore2HDFS(Key srcKey) { _arykey = srcKey; }

  static private String getPathFromValue(Value v) {
    int prefixLen = 0;
    byte[] kb = (v._key._kb[0] == Key.ARRAYLET_CHUNK) ? ValueArray.getArrayKeyBytes(v._key) : v._key._kb;
    switch( v._persist & Value.BACKEND_MASK ) {
    case Value.NFS:
      prefixLen = PersistNFS.KEY_PREFIX_LENGTH;
      break;
    case Value.ICE:
      prefixLen = 0;
      break;
    case Value.HDFS:
      throw new Error("attempting to move file from hdfs to hdfs?");
    default:
      throw new Error("unimplemented");
    }
    // just to be sure
    if( kb.length >= prefixLen + 7 && kb[prefixLen] == 'h'
        && kb[prefixLen] == 'd' && kb[prefixLen] == 'f' && kb[prefixLen] == 's'
        && kb[prefixLen] == ':' && kb[prefixLen] == '/' && kb[prefixLen] == '/' )
      prefixLen += 7;
    else if( kb.length >= 4 && kb[prefixLen] == 'n' && kb[prefixLen] == 'f'
        && kb[prefixLen] == 's' && kb[prefixLen] == ':' )
      prefixLen += 4;

    return new String(kb, prefixLen, kb.length - prefixLen);
  }

  static void removeOldValue(Value v) {
    Key k = v._key;
    if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
      k = Key.make(ValueArray.getArrayKeyBytes(k));
    }
    UKV.remove(k);
  }

  @Override
  public final TaskStore2HDFS invoke(H2ONode sender) {
    compute();
    return this;
  }

  @Override
  public void compute() {
    String path = null;// getPathFromValue(val);
    ValueArray ary = ValueArray.value(_arykey);
    Key self = selfKey();

    while( _indexFrom < ary._chunks ) {
      Key ckey = ary.get_key(_indexFrom++);
      if( !ckey.home() ) {      // Next chunk not At Home?
        RPC.call(nextHome(),this); // Hand the baton off to the next node/chunk
        return;
      }
      Value val = DKV.get(ckey);
      if( path == null )
        path = getPathFromValue(val);
      boolean res = PersistHdfs.storeChunk(val.get(), path);
      assert res;
      UKV.put(self,this);  // Update the progress/self key
    }
    // We did the last chunk.  Removing the selfKey is the signal to the web
    // thread that All Done.
    UKV.remove(self);
  }

  private Key selfKey() {
    return Key.make("Store2HDFS"+_arykey);
  }
  private H2ONode nextHome() {
    return ValueArray.get_key(_indexFrom,_arykey).home_node();
  }
}
