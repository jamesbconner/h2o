/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.RandomTree;
import hexlytics.data.Data;

/** Class that is capable of building the random forest trees.
 * 
 * The trees are build one by one and whenever a tree is built, the glue
 * object is notified. 
 *
 * @author peta
 */
public class Builder implements Runnable {
  
  final BuilderGlue glue_;
  final Data data_;
  final double bagSize_;
  Thread[] threads_ = null;
  int runningThreads_ = 0;
  
  volatile boolean terminate_ = false;
  
  /** Creates the builder object with given arguments and associated glue object.
   */
  public Builder(Data data, double bagSize, BuilderGlue glue) {
    data_ = data;
    bagSize_ = bagSize;
    glue_ = glue;
  }

  /** Starts the builder in current thread. */
  public void start() {
    run();
  }
  
  /** Starts the builder in N new threads. */
  public void start(int threads) {
    threads_ = new Thread[threads];
    for (int i = 0; i<threads; ++i) {
      threads_[i] = new Thread(this);
      threads_[i].start();
    }
  }
  /** Terminates the builder, after its next tree (or trees for each thread one
   * are created). That is terminates the builder in fastest safe way possible
   */
  public void terminate() {
    terminate_ = true;
  }
  
<<<<<<< HEAD
  /** Starts computing the trees. This method should not be called from outside,
   * use the method start() instead.
   */
  @Override public void run() {
    // increase the number of workers
    synchronized (this) {
      ++runningThreads_;
    }
    // compute the trees one at a time, each one with newly sampled data
=======
  
  
   public void run() {
>>>>>>> override override
    while (terminate_ == false) {
      Data d = data_.sampleWithReplacement(bagSize_);
      RandomTree tree = new RandomTree(d);
      glue_.onTreeReady(tree);
    }
    // only the last builder to terminate should call the terminated event on
    // the glue object
    boolean isLast;
    synchronized (this) {
      isLast = (--runningThreads_ == 0);
    }
    if (isLast)
      glue_.onBuilderTerminated();
  }
  
}
