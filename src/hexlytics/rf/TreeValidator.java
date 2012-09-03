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
   String s = glue_.nodeName()+" "+rf_.trees_.size()+" Time: "+tree.time_+" Tree depth = "+tree.tree_.depth()+" leaves= "+ tree.tree_.leaves();
    glue_.report(s+" | err="+ Utils.p5d(err)+" Tree="+ts);
  }

  /** We are done. Finish any validation and send you date to the aggregator. */
  public void terminate() {
    String s = 
        "              Type of random forest: classification\n" +
        "                    Number of trees: "+ rf_.trees_.size() +"\n"+
        "No of variables tried at each split: " + DataAdapter.FEATURES+"\n"+
        "             Estimate of error rate: " + Math.round(err *10000)/100 + "%\n"+ 
        "                   Confusion matrix:\n" + rf_.confusionMatrix();
    glue_.report(s);
  }
  
}
