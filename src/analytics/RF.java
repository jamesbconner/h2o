package analytics;


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

}
