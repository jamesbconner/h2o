package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.Tree;
import hexlytics.data.Data;
import java.util.concurrent.atomic.AtomicInteger;



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

  private final Data trainingData_;
  private final double sampleSize_;

  private final Director glue_;
  
  private volatile boolean terminate_ = false;
  private AtomicInteger threads_ = new AtomicInteger(0);
  
  private final int maxTrees_;
  
  private AtomicInteger trees_ = new AtomicInteger(0);
  
  
  public TreeBuilder(Data trainingData, Director glue) {
    this(trainingData, 1.0, glue,-1);
  }
  
  public TreeBuilder(Data trainingData, Director glue, int maxTrees) {
    this(trainingData, 1.0, glue,maxTrees);
  }
  
  public TreeBuilder(Data trainingData, double sampleSize, Director glue, int maxTrees) {
    trainingData_ = trainingData;
    sampleSize_ = sampleSize;
    glue_ = glue;
    maxTrees_ = maxTrees;
  }
  
  public void terminate() {
    terminate_ = true;
  }
  
  
  @Override
  public void run() {
    // increase the number of threads we have 
    threads_.incrementAndGet();
    // build the trees until we should stop
    while (terminate_ == false) {
      // only build the tree if we haven't already built enough
      if (maxTrees_ !=-1) {
        if (trees_.incrementAndGet() > maxTrees_) {
          terminate();
          break;
        }
      }
      // get the data for the tree to be built
      Data d = (sampleSize_ == 1.0) ? trainingData_ : trainingData_.sampleWithReplacement(sampleSize_);
      Tree tree = new Tree();
      tree.compute(d);
      glue_.onTreeBuilt(tree);
    }    
    // decrement the number of threads and if we are the last one, call the event
    if (threads_.decrementAndGet()==0) {
      glue_.onBuilderTerminated();
      // wake anyone who has been waiting
      synchronized (this) {
        notify(); 
      }
    }
  }
  
}

/*

public class TreeBuilder {
  
    Director dir_;
    RandomForest rf_;
    
    public TreeBuilder(Data data, Director dir, int numTrees) { 
       rf_ = new RandomForest(data, dir_ = dir, numTrees);
       dir_.report("Training data\n"+ data.toString());
    }  
    
    public void build(){ rf_.build(); dir_.onBuilderTerminated(); }
    
} */
