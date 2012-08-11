/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.RandomTree;

/**
 *
 * @author peta
 */
public abstract class BuilderGlue {

  /** Builder object we talk to, set by the builder. */
  Builder builder_ = null;
  
  
  /** Called by the builder when new tree is computed. */
  public abstract void onTreeReady(RandomTree tree);
  
  /** Called by the builder when it terminates. */
  public abstract void onTerminated();
  
  /** Terminates the builder, after its next tree (or trees for each thread one
   * are created). That is terminates the builder in fastest safe way possible
   */
  public void terminate(boolean discardLastTrees) {
    
  }
  
}
