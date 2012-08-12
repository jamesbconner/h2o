package hexlytics.RFBuilder;

import hexlytics.Tree;
import hexlytics.Utils;
import hexlytics.data.Data;

/** This needs to be an interface so that we can inherit from the distributed
 * task.
 *
 * @author peta
 */
public interface Director {

  /** Called by the builder when new tree is computed. */
  void onTreeBuilt(Tree tree);
  
  /** Called by the builder when it terminates. */
  void onBuilderTerminated();

  /** Called by the aggregator when it has new results.  */
  void onAggregatorChange();  

  void onTreeValidated(Tree tree, int rows, int[] badRows, int[] badVotes);
  
  void onValidatorTerminated();
  
  void report(String what);
  
}


