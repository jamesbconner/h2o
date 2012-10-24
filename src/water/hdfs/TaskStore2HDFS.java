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
 * Optionally, progress can be monitored by passing a key to a value containing
 * number of chunks stored.  If not null, this value will be updated as
 * additional chunks are stored.
 *
 * @author tomasnykodym
 *
 */
public class TaskStore2HDFS extends RemoteTask {
  Key _key;
  long _indexFrom;
  final long _indexTo;
  final Key _progressK;

  public static Key store2Hdfs(Key srcKey) {
    Value v = DKV.get((srcKey._kb[0] == Key.ARRAYLET_CHUNK) ? Key.make(ValueArray.getArrayKeyBytes(srcKey)) : srcKey);
    String p = getPathFromValue(v);
    Key result = PersistHdfs.getKeyForPath(p);
    final long N = v.chunks();
    // For ValueArrays, make the .hex header
    if( v != null && v instanceof ValueArray ) {
      try {         // for .hex files we need to store the header with metadata
        PersistHdfs.createFile(v, p);
      } catch( IOException e ) {
        throw new Error(e);
      }
    }
    // The progress key
    Key pK = Key.make(Key.make()._kb, (byte) 1, Key.DFJ_INTERNAL_USER, H2O.SELF);
    Value progress = new Value(pK, 8);
    DKV.put(pK, progress);

    try {
      TaskStore2HDFS tsk = new TaskStore2HDFS(0, N, srcKey, pK);
      tsk.invoke((v instanceof ValueArray) ? ValueArray.make_chunkkey(srcKey,0) : srcKey);
      // HTML file save of Value
      long storedCount = 0;
      while( (storedCount = UDP.get8(DKV.get(pK).get(), 0)) < N ) {
        try { Thread.sleep(100); } catch( InterruptedException e ) { }
      }
    } finally {
      // remove progress info
      DKV.remove(pK);
    }
    PersistHdfs.refreshHDFSKeys();
    return result;
  }

  public TaskStore2HDFS(long indexFrom, long indexTo, Key srcKey, Key progressKey) {
    _key = srcKey;
    _indexFrom = indexFrom;
    _indexTo = indexTo;
    _progressK = progressKey;
  }

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
  public final void invoke(Key key) {
    _key = key;
    compute();
  }

  @Override
  public void compute() {
    String path = null;// getPathFromValue(val);
    while( _key.home() ) {
      Value val = UKV.get(_key);
      // first store the data (so that the cleaner does not get into our way)
      if( path == null )
        path = getPathFromValue(val);
      try {
        PersistHdfs.storeChunk(val, path);
      } catch( IOException e ) {
        throw new Error(e);
      }
      // val.switch2HdfsBackend(true); // switch the value persist backend to
      // hdfs and status to ON DISK
      if( ++_indexFrom == _indexTo ) { // all done, load value to the store
        try {
          PersistHdfs.importPath(path);
        } catch (IOException e) {
          throw new Error(e);
        }
        // remove the old value
        removeOldValue(val);
        break;
      }
      _key = ValueArray.getChunk(val._key, _indexFrom);
    }
    // store the rest
    if( _indexFrom < _indexTo )
      new TaskRemExec<TaskStore2HDFS>(_key.home_node(), this, _key);
    // update the progress info
    if( _progressK != null ) {
      AtomicMax amax = new AtomicMax(_indexFrom);
      amax.invoke(_progressK);
    }
  }

  public static class AtomicMax extends Atomic {
    private final long _myVal;
    public AtomicMax(long val) { _myVal = val; }

    @Override
    public byte[] atomic(byte[] bits) {
      assert bits != null;
      byte[] res = new byte[8];
      UDP.set8(res, 0, Math.max(_myVal, UDP.get8(bits, 0)));
      return res;
    }
  }
}
