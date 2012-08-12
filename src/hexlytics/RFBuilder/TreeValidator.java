package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.Tree;
import hexlytics.data.Data;

/**
 *
 * @author peta
 */
public class TreeValidator {
  
  final Director glue_;
  RandomForest rf_;
  
  public TreeValidator(Data data, Director glue) {
    glue_ = glue;
    rf_= new RandomForest(data,glue_,Integer.MAX_VALUE);
  }
  
  /** Adds the given tree to the queue of trees to be validated. */
  public double validateTree(Tree tree) { return rf_.validate(tree); }
      
}
