package hexlytics.rf;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/** Is capable of validating a given tree against given data. 
 * 
 * Similar to the TreeBuilder can run in multiple threads, but does not have to 
 * 
 * @author peta
 */
public class TreeValidator {
  private final Director _director;
  private final Data _data;
  public final RandomForest _rf;
  private double err;
  
  public TreeValidator(Data data, Director glue) {
    _director = glue;
    _data = data;
    _rf = new RandomForest(data, glue, Integer.MAX_VALUE);
  }
  
  
  public void validate(Tree tree) { 
    err = _rf.validate(tree); 
    String ts = tree.toString();    
    if(ts.length()>=100) ts = ts.substring(0,100) + "...";    
    String s = _director.nodeName()+
        " "+_rf.trees_.size()+
        " Time: "+tree.time_+
        " Tree depth = "+tree.tree_.depth()+
        " leaves= "+ tree.tree_.leaves();
    _director.report(s+" | err="+ Utils.p5d(err)+" Tree="+ts);
  }

  public void terminate() {
    String s = 
        "              Type of random forest: classification\n" +
        "                    Number of trees: "+ _rf.trees_.size() +"\n"+
        "No of variables tried at each split: " + _data.features() +"\n"+
        "             Estimate of error rate: " + Math.round(err *10000)/100 + "%\n"+ 
        "                   Confusion matrix:\n" + _rf.confusionMatrix();
    _director.report(s);
    try {
      PrintWriter pw = new PrintWriter(new File("/tmp/foo.dot"));
      TreePrinter tp = new TreePrinter(pw);
      tp.printForest(_rf);
      pw.close();
    } catch( IOException e ) {
      throw new RuntimeException(e);
    }
  }
  
}
