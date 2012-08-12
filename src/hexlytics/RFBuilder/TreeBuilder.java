package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.data.Data;

/** Tree builder builds trees and returns to the director. The underlying build
 * is done multi threaded by the RandomForest class.
 * 
 * @author peta
 */
public class TreeBuilder implements Runnable {

    private final Director dir_;
    private final RandomForest rf_;
    
    public TreeBuilder(Data data, Director dir, int numTrees) { 
       rf_ = new RandomForest(data, dir_ = dir, numTrees);
       dir_.report("Training data\n"+ data.toString());
    }      
    public void run(){ rf_.build(); dir_.onBuilderTerminated(); }
    public void terminate() { rf_.terminate();} 
}
