package analytics;

/**
 *
 * @author peta
 */
public class RF implements Classifier {
  private final DecisionTree[] trees_;
  
  RF(DecisionTree[] trees) { trees_ = trees;  assert trees != null && trees.length>=1;  }

  public int classify(DataAdapter data) {
    int[] counts = new int[numClasses()];
    for (DecisionTree tree: trees_)
      counts[tree.classify(data)] += 1;
    return Utils.maxIndex(counts);
  }

  public int numClasses() { return trees_[0].numClasses(); }
  
  public static RF compute(int numTrees, RFBuilder builder) { return builder.compute(numTrees);  }

  
  public DecisionTree tree(int n) {
    return trees_[n];
  }
}