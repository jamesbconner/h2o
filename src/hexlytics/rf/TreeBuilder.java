package hexlytics.rf;


/** Tree builder builds trees and returns to the director. The underlying build
 * is done multi threaded by the RandomForest class.
 * 
 * @author peta
 */
public class TreeBuilder implements Runnable {

    private final boolean gini_;
  
    private final Director dir_;
    private final RandomForest rf_;
    
    public TreeBuilder(Data data, Director dir, int numTrees) { 
      gini_ = false;
      rf_ = new RandomForest(data, dir_ = dir, numTrees);
      dir_.report("Training data\n"+ data.toString());
    }      

    public TreeBuilder(Data data, Director dir, int numTrees, boolean gini) { 
      gini_ = gini;
      rf_ = new RandomForest(data, dir_ = dir, numTrees);
      dir_.report("Training data\n"+ data.toString());
    }      
    public void run(){ if (gini_) rf_.buildGini(); else rf_.build();  }
    public void terminate() { rf_.terminate();} 
    
    public int size() {return rf_.trees_.size();}
}


