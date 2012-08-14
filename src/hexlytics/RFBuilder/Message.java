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

  byte[] serialize() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = new ObjectOutputStream(bos);
    out.writeObject(this);
    byte[] buf = bos.toByteArray();
    out.close();
    bos.close();
    return buf;
  }

  public void send() {
    try {
      System.out.println("Sending " + toString());
      Key k = Key.make(PokerDRF._nodePrefix + nodeId_
          + this.getClass().toString());
      Value newV = new Value(k, serialize());
      DKV.put(k, newV);
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  private static Value readNext(String msgClassName, Value[] previousValues) {
    for (H2ONode node : H2O.CLOUD._memary) {
      int idx = H2O.CLOUD.nidx(node);
      Key k = Key.make(PokerDRF._nodePrefix + idx + msgClassName);
      Value v = DKV.get(k);
      if (v == null)
        return null;
      if (v != previousValues[H2O.CLOUD.nidx(node)]) {
        byte[] mem = v.get();
        if (mem != null) {
          previousValues[idx] = v;
          return v;
        } else
          System.err.println("got null value for key " + k);
      }
    }
    return null;
  }

  public static Message readNext(Class c, Value [] previousValues) {    
    Value v = Message.readNext(c.toString(), previousValues);
    if (v == null)
      return null;
    byte[] m = v.get();
    v.free_mem();
    // DKV.remove(k); can cause exception now
    return (Message)deserialize(m);
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

  }

  public static class ValidationError extends Message {
    public long val_;
  }

  public static class Text extends Message {
    public String val_;

    public Text(String val) {
      val_ = val;
    }

    private static Value[] _previousValues = null;

    public static Text readNext() {
      if (_previousValues == null)
        _previousValues = new Value[H2O.CLOUD.size()];      
      return (Text) readNext(Text.class,_previousValues);
    }

    public String toString() {
      return super.toString() + ":" + val_;
    }
  }

  public static class Tree extends Message {
    public int treeId_;
    public hexlytics.Tree tree_;

    void printToFile() {
      File f = new File(PokerDRF._nodePrefix + ".rf");
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

    private static Value[] _previousValues = null;

    public static Tree readNext() {
      if (_previousValues == null)
        _previousValues = new Value[H2O.CLOUD.size()];      
      return (Tree)readNext(Tree.class, _previousValues);
    }
    
    public String toString() {
      return super.toString() + " tree(" + treeId_ + ") " + tree_.toString();
    }
  }
}
