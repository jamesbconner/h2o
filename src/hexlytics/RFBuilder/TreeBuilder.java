package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.data.Data;

public class TreeBuilder {
  
    Director dir_;
    RandomForest rf_;
    
    public TreeBuilder(Data data, Director dir, int numTrees) { 
       rf_ = new RandomForest(data, dir_ = dir, numTrees);
       dir_.report("Training data\n"+ data.toString());
    }  
    
    public void build(){ rf_.build(); dir_.onBuilderTerminated(); }
    
}
