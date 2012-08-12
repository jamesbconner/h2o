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
  int treeIndex = 0;

  
  public void report(String s) { pln(s);  }
  
  protected void p(String s){ System.out.print(s); }
  protected void pln(String s){ System.out.println(s); }
    
  public LocalBuilder(Data t, Data v, int numTrees) {            
    pln("Training data:\n"+ t);
    pln("Validation data:\n"+ v);
    builder_ = new TreeBuilder(t,this,numTrees);
    validator_ = new TreeValidator(v,this);
    aggregator_ = new TreeAggregator(1,v.classes(),this);
    pln("===Computing===");
    builder_.run();
    System.out.println("DONE ALL: total error: "+aggregator_.getError());
  }
  
  public void onTreeBuilt(Tree tree) { 
    ++treeIndex;
    double err = validator_.validate(tree);  
    String ts = tree.toString();
    if(ts.length()>=100) ts = ts.substring(0,100);
    pln(treeIndex + " | err=" + Utils.p5d(err) + " " + ts);
  }

  public void onTreeValidated(Tree tree, int rows, int[] badRows, int[] badVotes) {
    aggregator_.aggregate(0,tree,rows,badRows,badVotes);
  }
  
  public void onAggregatorChange() {

  }
  
  
  public void onBuilderTerminated() {
    System.out.println("builder terminated...");
  }

  
  public void onValidatorTerminated() {
    System.out.println("validator terminated...");
  }

}