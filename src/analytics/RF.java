/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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

}
/*
public class RF {
    private final DataAdapter data_;
    public RF(DataAdapter data) { data_ = data;  }
    
    public DecisionTree[] compute(int ntrees, RFBuilder b) { 
      b.compute(ntrees,false);
      System.out.println("Testing " +(ntrees) + " trees");
      for (int t = 0;  t< ntrees; ++t) {
        DecisionTree dt = new DecisionTree(b.trees[t].root_);
        for (int r = 0; r< data_.numRows(); ++r) {
          data_.seekToRow(r);
          int expected =data_.dataClass();
          data_.seekToRow(r);
          int got = dt.classify(data_);
          if (got!=expected) 
            System.out.println(" Row "+r+" expected "+expected+", got "+got);
        }
      }
      return null;
    }  

} */
