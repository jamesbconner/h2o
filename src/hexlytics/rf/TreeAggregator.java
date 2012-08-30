package hexlytics.rf;


import java.util.ArrayList;

/** Aggregates results from the validators and builders. The aggregate results can then
 * be reported to the user.
 *  @author peta
 */
class TreeAggregator {

  public void onReport(String s) { System.out.println(s); }
  
  /** We are done... print the error rate...*/
  public void terminate() { } 
  
 
  private int totalRows_ = 0;
  private final int numClasses_;
   private final ArrayList<Tree> trees_ = new ArrayList(); // no need to be a set, since there is no real EQ method on trees
  private final Director glue_;
  
  
  public TreeAggregator(int numChunks, int numClasses, Director glue) {
    numClasses_ = numClasses;
     glue_ = glue;
  }
  
  public void aggregate( Tree tree) {
    // add the tree if we haven't seen it before
    trees_.add(tree);
  
  }

   
}