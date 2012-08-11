/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.RandomTree;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author peta
 */
public class Validator implements Runnable {
  
  final Data data_;
  final ValidatorGlue glue_;
  final LinkedBlockingQueue<RandomTree> trees_ = new LinkedBlockingQueue();
  
  private final static RandomTree TERMINATE = new RandomTree();
  
  
  private volatile boolean terminate_ = false;
  private int runningThreads_ = 0;
  private Thread[] threads_ = null;
  
  public Validator(Data data, ValidatorGlue glue) {
    data_ = data;
    glue_ = glue;
  }
  
  /** Adds the given tree to the queue of trees to be validated. */
  public void validateTree(RandomTree tree) {
    trees_.offer(tree);
  }
  
  public void start() {
    run();
  }
  
  public void start(int threads) {
    threads_ = new Thread[threads];
    for (int i = 0; i<threads; ++i) {
      threads_[i] = new Thread(this);
      threads_[i].start();
    }
    
  }

  /** Terminate all threads of the validator. */
  public void terminate() {
    for (int i = 0; i<runningThreads_; ++i)
      trees_.offer(TERMINATE); // to make sure all threads will die
    terminate_ = true;
  }
    
  /** Get trees one by one and validate them on given data. */
  @Override public void run() {
    // increase the number of workers
    synchronized (this) {
      ++runningThreads_;
    }
    // get the tree and validate it on given data
    while (true) {
      RandomTree tree;
      try {
        tree = trees_.take();
        if ((tree == TERMINATE) || (terminate_ == true))
          break;
        // we have a correct tree, validate it on data
        int errors = 0;
        int[] votes = new int[data_.rows()];
        for (Row r : data_) {
          votes[r.index] = tree.classify(r);
          if (votes[r.index]!=r.classOf)
            ++errors;
        }
        glue_.onTreeValidated(tree, data_.rows(), errors, votes);
      } catch( InterruptedException ex ) {
        // pass
      }
    }
    // only the last builder to terminate should call the terminated event on
    // the glue object
    boolean isLast;
    synchronized (this) {
      isLast = (--runningThreads_ == 0);
    }
    if (isLast)
      glue_.onValidatorTerminated();
  }
}
