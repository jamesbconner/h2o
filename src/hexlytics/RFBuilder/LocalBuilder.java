/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.Tree;
import hexlytics.Utils;
import hexlytics.data.Data;

/**
 *
 * @author peta
 */
// Peta: just a simple paste here, should be way different
public class LocalBuilder implements Director {
  TreeBuilder builder_;
  TreeValidator validator_;
  TreeAggregator aggregator_;

  @Override
  public void report(String s) { pln(s);  }
  
  protected void p(String s){ System.out.print(s); }
  protected void pln(String s){ System.out.println(s); }
    
    public LocalBuilder(Data t, Data v, int numTrees) {            
      pln("Training data:\n"+ t);
      pln("Validation data:\n"+ v);
      builder_ = new TreeBuilder(t,this,numTrees);
      validator_ = new TreeValidator(v,this);
      aggregator_ = new TreeAggregator(this);
      pln("===Computing===");
      builder_.build();
    }
    
    
    public void onTreeReady(Tree tree) { 
      double err = validator_.validateTree(tree);  
      String ts = tree.toString();
      if(ts.length()>=100) ts = ts.substring(0,100);
      pln(validator_.rf_.trees_.size() + " | err=" + Utils.p5d(err) + " " + ts);
    }
  
    public void onTreeValidated(Tree tree, int rows, int[] errorRows) {
      aggregator_.aggregateTree(tree, errorRows);
    }

    public void onChange() {

    }

  @Override
  public void onBuilderTerminated() {
    System.out.println("builder terminated...");
  }

  @Override
  public void onValidatorTerminated() {
    System.out.println("validator terminated...");
  }

}