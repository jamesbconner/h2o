package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.data.Data;

/** Tree builder is very simple class now that only builds the trees and returns
 * them to the glue object.
 * 
 * It is always single threaded in essence, but multiple threads can run the
 * same builder. This means it should be much easier to add it to the FJ tasks,
 * etc. you just use it with new thread object. 
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
