/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.RandomTree;
import hexlytics.data.Data;

/**
 *
 * @author peta
 */
public class Builder implements Runnable {
  
  final BuilderGlue glue_;
  final Data data_;
  final double bagSize_;
  Thread[] threads_;
  
  volatile boolean terminate_ = false;
  
  /** Creates the builder object and runs it in a  */
  public Builder(Data data, double bagSize, BuilderGlue glue) {
    data_ = data;
    bagSize_ = bagSize;
    glue_ = glue;
    glue_.builder_ = this;
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
  
  
  
  @Override public void run() {
    while (terminate_ == false) {
      Data d = data_.sampleWithReplacement(bagSize_);
      RandomTree tree = new RandomTree(d);
      glue_.onTreeReady(tree);
    }
    glue_.onTerminated();
  }
  
}
