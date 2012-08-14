/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.Tree;
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

  
  public void report(String s) { aggregator_.onReport(s);  }  
  protected void p(String s){ aggregator_.onReport(s); }
    
  public LocalBuilder(Data t, Data v, int numTrees) {            
    aggregator_ = new TreeAggregator(1,v.classes(),this);
    p("Training data:\n"+ t+"\nValidation data:\n"+ v);
    builder_ = new TreeBuilder(t,this,numTrees);
    validator_ = new TreeValidator(v,this);
    p("===Computing===");
    builder_.run();
    validator_.terminate();
    aggregator_.terminate();    

  }
  
  public void onTreeBuilt(Tree tree) { 
    ++treeIndex;
    validator_.validate(tree);  
  }

  public void onTreeValidated(Tree tree, int rows, int[] badRows, int[] badVotes) {
    aggregator_.aggregate(0,tree,rows,badRows,badVotes);
  }
  
  public void onAggregatorChange() { }

  public void onBuilderTerminated() { }

  public void onValidatorTerminated() { }
  
  public String nodeName() { return ""; }
  
  public void error(long error) {
    // TODO Auto-generated method stub
    
  } 

}