package analytics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.DecimalFormat;

/**
 *
 * @author peta
 */
public class RF { // implements Classifier {
  private static final long serialVersionUID = 3449080587112104147L;
  private DecisionTree[] trees_;
  final RFBuilder builder_;
  final DataAdapter data_;
  final int numTrees_;
  long time_;
  
  public RF(DataAdapter data, int numtrees) { 
    data_=data; numTrees_=numtrees; builder_= new RFBuilder(data);
  }
  
  public int classify(DataAdapter data) {
    int[] counts = new int[numClasses()];
    for (DecisionTree tree: trees_)
      counts[tree.classify(data)] += 1;
    return Utils.maxIndex(counts, data_.random_);
  }

  public int numClasses() { return trees_[0].numClasses(); }
  
  public void compute() { 
    long t1 = System.currentTimeMillis();
    trees_ = builder_.compute(numTrees_);
    long t2 = System.currentTimeMillis();
    time_ = (t2-t1);
  }
  public double outOfBagError() { return builder_.outOfBagError(); }
  public int numTrees() { return trees_.length; }
  public DecisionTree tree(int n) { return trees_[n];  }
  private byte[] _trees;
  public byte[] trees() throws IOException {  
    return _trees!=null? _trees : ( _trees=serialize(trees_)); 
  }  
  public void combine(byte[] ts) throws IOException, ClassNotFoundException {
    DecisionTree[] other = (DecisionTree[]) deserialize(ts);
    DecisionTree[] merged = new DecisionTree[other.length+trees_.length];
    for(int i=0,j=0;i<trees_.length;i++,j++) merged[j]=trees_[i];
    for(int i=0,j=trees_.length;i<other.length;i++,j++) merged[j]=other[i];
    trees_ = merged;
  }  
  static byte[] serialize(Object q) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = new ObjectOutputStream(bos);
    out.writeObject(q);
    byte[] buf = bos.toByteArray();    
    out.close();
    bos.close();
    return buf;
  }


  private Object deserialize(byte[] mem) throws IOException, ClassNotFoundException {
    return  new ObjectInputStream(new ByteArrayInputStream(mem)).readObject();   
  }
  
  static final DecimalFormat df = new  DecimalFormat ("0.###");
 
  public String toString() {
    String errors="";
    for (int i = 0; i<numTrees(); ++i) 
       errors +=" " +  df.format(Classifier.Operations.error(tree(i),data_));
    return "RF:  " + trees_.length + " trees, seed="+ data_.seed_ +", compute(ms)="+time_+"\n"
        + "#nodes="+ DecisionTree.nodeCount + "\n"
        + "OOB err = " + outOfBagError() + "\n";// + "Single tree errors: " + errors;
  }
}

