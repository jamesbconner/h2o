/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.data.Data;

/** Class that is capable of building the random forest trees.
 * 
 * The trees are build one by one and whenever a tree is built, the glue
 * object is notified. 
 *
 * @author peta
 */
public class Builder implements Runnable {
  
  final RandomForest rf_;
  final BuilderGlue glue_;
  volatile boolean terminate_ = false;
  
  /** Creates the builder object with given arguments and associated glue object. */
  public Builder(Data data, BuilderGlue glue, int trees) {
    rf_ = new RandomForest(data, trees);
    glue_ = glue;
  }

  
  /** Terminates the builder, after its next tree (or trees for each thread one
   * are created). That is terminates the builder in fastest safe way possible
   */
  public void terminate() { rf_.terminate(); }
  
   public void run() {
     rf_.build();
///     glue_.onTreeReady(tree);
       glue_.onBuilderTerminated();
  }
  
}
