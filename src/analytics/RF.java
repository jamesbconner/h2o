package analytics;

import java.text.DecimalFormat;

/**
 *
 * @author peta
 */
public class RF implements Classifier {
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

  public int numTrees() {
    return trees_.length;
  }
  
  public DecisionTree tree(int n) {
    return trees_[n];
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