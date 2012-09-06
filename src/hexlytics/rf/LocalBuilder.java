package hexlytics.rf;
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
    
  public LocalBuilder(Data t, Data v, int numTrees, boolean gini) {            
    aggregator_ = new TreeAggregator(1,v.classes(),this);
    p("Training data:\n"+ t+"\nValidation data:\n"+ v);
    builder_ = new TreeBuilder(t,this,numTrees,gini);
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
    aggregator_.aggregate(tree);
  }
  
  public String nodeName() { return ""; }
  
  public void error(long error) {
    // TODO Auto-generated method stub
    
  } 

}