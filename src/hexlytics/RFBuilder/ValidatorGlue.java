/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.Tree;

/**
 *
 * @author peta
 */
public interface ValidatorGlue {
  
  void onTreeValidated(Tree tree, int rows, int[] errorRows);
  
  void onValidatorTerminated();
  
}
