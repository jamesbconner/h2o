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
public interface ValidatorGlue {
  
  void onTreeValidated(RandomTree tree, int rows, int[] errorRows);
  
  void onValidatorTerminated();
  
}
