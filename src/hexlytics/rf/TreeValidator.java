package hexlytics.rf;

/** Is capable of validating a given tree against given data. 
 * 
 * Similar to the TreeBuilder can run in multiple threads, but does not have to 
 * 
 * @author peta
 */
public class TreeValidator {
  
  final Director glue_;
  public RandomForest rf_;
  
  public TreeValidator(Data data, Director glue) {
    glue_ = glue;
    rf_= new RandomForest(data,glue_,Integer.MAX_VALUE);
  }
  
  double err;
  
  public void validate(Tree tree) { 
    err = rf_.validate(tree); 
    String ts = tree.toString();    
    if(ts.length()>=100) ts = ts.substring(0,100) + "...";
    glue_.report(glue_.nodeName()+" "+rf_.trees().size()+" | err="+ Utils.p5d(err)+" Tree="+ts);
  }

  /** We are done. Finish any validation and send you date to the aggregator. */
  public void terminate() {
    glue_.report("Err=" + err);
    glue_.error(rf_.errors()); 
  }
  
}
