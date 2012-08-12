package hexlytics.RFBuilder;

import hexlytics.Tree;
import hexlytics.Utils;
import hexlytics.data.Data;

/**
 *
 * @author peta
 */
public abstract class Director {

  /** Called by the builder when new tree is computed. */
  public abstract void onTreeReady(Tree tree);
  
  /** Called by the builder when it terminates. */
  public  void onBuilderTerminated() {}

  /** Called by the aggregator when it has new results.  */
  abstract public void onChange();  

  public abstract void onTreeValidated(Tree tree, int rows, int[] errorRows);
  
  public  void onValidatorTerminated() {}
  
  public static Director createLocal(Data train, Data valid, int numTrees) {
    return new Local(train, valid, numTrees);
  }
  
  public void report(String s) { pln(s);  }
  
  protected void p(String s){ System.out.print(s); }
  protected void pln(String s){ System.out.println(s); }
}

class Local extends Director {
  Builder builder_;
  Validator validator_;
  Aggregator aggregator_;
    
    public Local(Data t, Data v, int numTrees) {            
      pln("Training data:\n"+ t);
      pln("Validation data:\n"+ v);
      builder_ = new Builder(t,this,numTrees);
      validator_ = new Validator(v,this);
      aggregator_ = new Aggregator(this);
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

}