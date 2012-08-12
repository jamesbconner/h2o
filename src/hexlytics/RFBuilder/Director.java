package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.Tree;
import hexlytics.data.Data;

/**
 *
 * @author peta
 */
public abstract class Director {

  /** Called by the builder when new tree is computed. */
  public abstract void onTreeReady(Tree tree);
  
  /** Called by the builder when it terminates. */
  public abstract void onBuilderTerminated();

  /** Called by the aggregator when it has new results.  */
  abstract public void onChange();  

  public abstract void onTreeValidated(Tree tree, int rows, int[] errorRows);
  
  public abstract void onValidatorTerminated();
  
  public static Director createLocal(Data train, Data valid, int numTrees) {
    return new Local(train, valid, numTrees);
  }
  
  protected void p(String s){ System.out.print(s); }
  protected void pln(String s){ System.out.println(s); }
}

class Local extends Director {
  RandomForest builder_;
  Validator validator_;
  Aggregator aggregator_;
    
    public Local(Data t, Data v, int numTrees) {            
      pln("Training data:\n"+ t);
      pln("Validation data:\n"+ v);
      builder_ = new RandomForest(t,this,1);
      validator_ = new Validator(v,this);
      aggregator_ = new Aggregator(this);
      builder_.build();
    }
    
    
    public void onTreeReady(Tree tree) { validator_.validateTree(tree);
    }

    public void onBuilderTerminated() {
      System.out.println("builder terminated...");
    }

    public void onValidatorTerminated() {
      System.out.println("validator terminated...");
    }

    public void onTreeValidated(Tree tree, int rows, int[] errorRows) {
      aggregator_.aggregateTree(tree, errorRows);
    }

    public void onChange() {

    }

}