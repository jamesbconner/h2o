/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics;

import hexlytics.data.Data;

/**
 *
 * @author peta
 */
public class RandomForest {
  
  public final double bagSize;
  private RandomTree[] trees_;
  
  /** Creates new random forest with no trees. */
  public RandomForest(double bagSize) {
    trees_ = new RandomTree[0]; // so that we do not have the null guy
    this.bagSize = bagSize;
  }
  
  /** Creates the a random forest that is the same as already existing one. 
   * 
   * @param from 
   */
  public RandomForest(RandomForest from) {
    bagSize = from.bagSize;
    trees_ = new RandomTree[from.trees_.length];
    System.arraycopy(from.trees_, 0, trees_, 0, trees_.length);
  }

  /** Returns the number of the trees in the forest. */
  public int numTrees() {
    return trees_.length;
  }
  
  /* Computes N new trees and adds them to the forest. */
  public void addTrees(Data data, int numTrees) {
    long t1 = System.currentTimeMillis();
    RandomTree[] oldTrees = trees_;
    trees_ = new RandomTree[trees_.length+numTrees];
    new TreeBuilder(data,oldTrees.length,numTrees).run();
    System.arraycopy(oldTrees,0,trees_,0,oldTrees.length);
    t1 = System.currentTimeMillis() - t1;
    System.out.println(numTrees+" built in "+t1);
  }
  
  /** Adds the given */
  public void addTrees(Data data, int numTrees, int threads) {
    long t1 = System.currentTimeMillis();
    RandomTree[] oldTrees = trees_;
    trees_ = new RandomTree[trees_.length+numTrees];
    int[] tpt = Utils.splitEquallyBetween(numTrees,threads);
    TreeBuilder[] builders = new TreeBuilder[tpt.length];
    int offset = oldTrees.length;
    for (int i = 0; i< builders.length; ++i) {
      builders[i] = new TreeBuilder(data,offset,tpt[i]);
      offset += tpt[i];
      builders[i].start();
    }
    for (TreeBuilder builder : builders) 
      try { builder.join(); } catch (InterruptedException e) { }
    System.arraycopy(oldTrees,0,trees_,0,oldTrees.length);
    t1 = System.currentTimeMillis() - t1;
    System.out.println(numTrees+" built in "+t1);
  }
  
  
  
  
  
  private class TreeBuilder extends Thread {
    public final Data data;
    public final int firstTree;
    public final int numTrees;
    
    public TreeBuilder(Data data, int firstTree, int numTrees) {
      this.data = data.sampleWithReplacement(bagSize);
      this.firstTree = firstTree;
      this.numTrees = numTrees;
    }
    
    public void run() {
      //System.out.println("Building "+numTrees+" trees from tree "+firstTree);
      for (int i = firstTree; i<firstTree+numTrees; ++i) {
        trees_[i] = new RandomTree(data);
        trees_[i].compute();
      }
    }
  }
}
