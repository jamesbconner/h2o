package hexlytics.RFBuilder;

import hexlytics.Tree;

/** This needs to be an interface so that we can inherit from the distributed
 * task.
 *
 * @author peta
 */
public interface Director {

  /** Called by the builder when new tree is computed. */
  void onTreeBuilt(Tree tree);
 

  void report(String what);
  
  void error(long error);
  
  ///  
  String nodeName();
}


