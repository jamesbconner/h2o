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
public interface BuilderGlue {

  /** Called by the builder when new tree is computed. */
  void onTreeReady(RandomTree tree);
  
  /** Called by the builder when it terminates. */
  void onBuilderTerminated();
  
}
