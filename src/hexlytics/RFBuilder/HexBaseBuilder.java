/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.Tree;

/** This is the distributed builder of the random forest that works in hexbase.
 * 
 *
 *
 * @author peta
 */
public class HexBaseBuilder implements Director {

  @Override
  public void onTreeReady(Tree tree) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onBuilderTerminated() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onChange() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onTreeValidated(Tree tree, int rows, int[] errorRows) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onValidatorTerminated() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void report(String what) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
