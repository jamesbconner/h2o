package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.Tree;
import hexlytics.Utils;
import hexlytics.data.Data;


/** Is capable of validating a given tree against given data. 
 * 
 * Similar to the TreeBuilder can run in multiple threads, but does not have to 
 * 
 * @author peta
 */
public class TreeValidator {
  
  final Director glue_;
  RandomForest rf_;
  
  public TreeValidator(Data data, Director glue) {
    glue_ = glue;
    rf_= new RandomForest(data,glue_,Integer.MAX_VALUE);
  }
  
  public void validate(Tree tree) { 
    double res = rf_.validate(tree); 
    String ts = tree.toString();    
    if(ts.length()>=100) ts = ts.substring(0,100) + "...";
    glue_.report(glue_.nodeName()+" "+rf_.trees().size()+" | err="+ Utils.p5d(res)+" Tree="+ts);
  }

  /** We are done. Finish any validation and send you date to the aggregator. */
  public void terminate() { }
  
}
