package hexlytics.RFBuilder;

import hexlytics.tests.PokerDRF;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import water.DKV;
import water.H2O;
import water.H2ONode;
import water.Key;
import water.Value;

public abstract class Message implements Serializable {
  private static final long serialVersionUID = -5049273605286959641L;
  int nodeId_;

  public Message() {
    nodeId_ = H2O.CLOUD.nidx(H2O.SELF);
  }

  static Map<Class<? extends Message>, Integer> _nextSendIdx;
  static Map<Class<? extends Message>, int[]> _nextRcvIdxs;

  byte[] serialize() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = new ObjectOutputStream(bos);
    out.writeObject(this);
    byte[] buf = bos.toByteArray();
    out.close();
    bos.close();
    return buf;
  }

  static Key getMessageKey(Class<? extends Message> msgClass, int nodeIdx,
      int msgIdx) {
    return Key.make(PokerDRF.nodePrefix(nodeIdx) + msgClass.getSimpleName()
        + ((msgIdx == -1) ? "" : msgIdx));
  }

  int nextMessageIdx() {
    return -1;
  }

  public void send() {
    try {
      Key k = getMessageKey(this.getClass(), PokerDRF._nodeId, nextMessageIdx());
      Value newV = new Value(k, serialize());
      DKV.put(k, newV);
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  private static Message readNext(Class<? extends Message> msgClass,
      int[] msgIdxs) {
    for (H2ONode node : H2O.CLOUD._memary) {
      int idx = H2O.CLOUD.nidx(node);
      Key k = getMessageKey(msgClass, idx, (msgIdxs != null) ? msgIdxs[idx]
          : -1);
      Value v = DKV.get(k);
      if (v != null) {
        if (msgIdxs != null)
          ++msgIdxs[idx];
        return (Message) deserialize(v.get());
      }
    }
    return null;
  }

  private static Message deserialize(byte[] mem) {
    try {
      return (Message) new ObjectInputStream(new ByteArrayInputStream(mem))
          .readObject();
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  public String toString() {
    return Integer.toString(nodeId_);
  }

  public static class Command extends Message {
    private static final long serialVersionUID = 8779523369085751962L;
  }

  public static class Init extends Message {
    private static final long serialVersionUID = -374123216419814767L;
    public final String _nodePrefix;
    public final byte[] _kb; // input data
    public final int _nTrees;
    public final int _totalTrees;
    transient Key _initMsgKey;

    public Init(String nodePrefix, Key initMSgKey, Key dataKey, int nTrees,
        int totalTrees) {
      super();
      _nodePrefix = nodePrefix;
      _kb = dataKey._kb;
      _nTrees = nTrees;
      _totalTrees = totalTrees;
      _initMsgKey = initMSgKey;
    }

    public static Init readNext() {
      throw new UnsupportedOperationException();
    }

    public static Init read(Key k) {
      Value v = DKV.get(k);
      return (v != null) ? (Init) deserialize(v.get()) : null;
    }

    public void send() {
      try {
        Value newV = new Value(_initMsgKey, serialize());
        DKV.put(_initMsgKey, newV);
      } catch (IOException e) {
        throw new Error(e);
      }
    }

    public String toString() {
      return super.toString() + ":Init, NodePrefix=" + _nodePrefix
          + ", nTrees=" + _nTrees;
    }
  }

  public static class ValidationError extends Message {
    private static final long serialVersionUID = -8042422701584129050L;

    public ValidationError(long err, long nrecords) {
      err_ = err;
      nrecords_ = nrecords;
    }

    public long err_;
    public long nrecords_;    
    
    public static synchronized ValidationError readFrom(int nodeId) {      
      Key k = getMessageKey(ValidationError.class, nodeId, -1);
      Value v = DKV.get(k);
      return (v != null)?(ValidationError) deserialize(v.get()):null;      
    }
  }

  public static class Text extends Message {
    private static final long serialVersionUID = 8925306484581264590L;
    public String val_;

    static final AtomicInteger _nextSendMsgIdx = new AtomicInteger(0);

    static int[] _nextRecvIdxs;

    int nextMessageIdx() {
      return _nextSendMsgIdx.getAndIncrement();
    }

    public Text(String val) {
      val_ = val;
    }

    public static synchronized Text readNext() {
      if (_nextRecvIdxs == null)
        _nextRecvIdxs = new int[H2O.CLOUD.size()];
      return (Text) readNext(Text.class, _nextRecvIdxs);
    }

    public String toString() {
      return super.toString() + ":" + val_;
    }
  }

  public static class Tree extends Message {
    private static final long serialVersionUID = 598521071395186038L;
    public int treeId_;
    public hexlytics.Tree tree_;

    static int[] _nextRecvIdxs;

    static final AtomicInteger _nextSendMsgIdx = new AtomicInteger(0);

    void printToFile() {
      File f = new File(PokerDRF.nodePrefix(0) + ".rf");
      try {
        if (!f.exists())
          f.createNewFile();
        FileWriter out = new FileWriter(f, true);
        out.append(tree_.toString() + "\n");
        out.close();
      } catch (IOException e) {
        System.err.println("File output failed " + e);
      }
    }

    public Tree(int id, hexlytics.Tree t) {
      treeId_ = id;
      tree_ = t;
    }

    int nextMessageIdx() {
      return _nextSendMsgIdx.getAndIncrement();
    }

    public static synchronized Tree readNext() {
      if (_nextRecvIdxs == null)
        _nextRecvIdxs = new int[H2O.CLOUD.size()];
      return (Tree) readNext(Tree.class, _nextRecvIdxs);
    }

    public String toString() {
      return super.toString() + " tree(" + treeId_ + ") " + tree_.toString();
    }
  }
}
