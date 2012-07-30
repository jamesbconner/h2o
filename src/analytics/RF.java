package analytics;

/**
 *
 * @author peta
 */
public class RF implements Classifier {
  private DecisionTree[] trees_;
  final int seed_;
  final RFBuilder builder_;
  final DataAdapter data_;
  final int numTrees_;
  long time_;
  
  public RF(DataAdapter data, RFBuilder builder, int numtrees, int seed) { 
    builder_=builder; data_=data; seed_ = seed; numTrees_=numtrees;
    builder.setSeed(seed_);
  }
  public int classify(DataAdapter data) {
    int[] counts = new int[numClasses()];
    for (DecisionTree tree: trees_)
      counts[tree.classify(data)] += 1;
    return Utils.maxIndex(counts);
  }

  public int numClasses() { return trees_[0].numClasses(); }
  public void compute() { 
    long t1 = System.nanoTime();
    trees_ = builder_.compute(numTrees_);
    long t2 = System.nanoTime();
    time_ = (t2-t1)/1000000;
  }
  public double outOfBagError() { return builder_.outOfBagError(); }

  public String toString() {
    return "RF:  " + trees_.length + " trees, seed="+ seed_ +", compute(ms)="+time_+"\n"
        + "OOB err = " + outOfBagError() + "\n";
  }
}