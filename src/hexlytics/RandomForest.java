package hexlytics;

import hexlytics.data.Data;
import hexlytics.data.Data.Row;

import java.util.Random;

/**
 *
 * @author peta
 */
public class RandomForest {
  
  private static Random rnd = new Random();
  
  public final double bagSize;
  private RandomTree[] trees_;
  private RandomTree[] treesUnderConstruction_;
  
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
    treesUnderConstruction_ = new RandomTree[trees_.length+numTrees];
    new TreeBuilder(data,trees_.length,numTrees).run();
    System.arraycopy(trees_,0,treesUnderConstruction_,0,trees_.length);
    trees_ = treesUnderConstruction_;
    treesUnderConstruction_ = null;
    t1 = System.currentTimeMillis() - t1;
    System.out.println(numTrees+" built in "+t1);
  }
  
  /** Adds the given */
  public void addTrees(Data data, int numTrees, int threads) {
    long t1 = System.currentTimeMillis();
    treesUnderConstruction_ = new RandomTree[trees_.length+numTrees];
    int[] tpt = Utils.splitEquallyBetween(numTrees,threads);
    TreeBuilder[] builders = new TreeBuilder[tpt.length];
    int offset = trees_.length;
    for (int i = 0; i< builders.length; ++i) {
      builders[i] = new TreeBuilder(data,offset,tpt[i]);
      offset += tpt[i];
      builders[i].start();
    }
    for (TreeBuilder builder : builders) 
      try { builder.join(); } catch (InterruptedException e) { }
    System.arraycopy(trees_,0,treesUnderConstruction_,0,trees_.length);
    trees_ = treesUnderConstruction_;
    treesUnderConstruction_ = null;
    t1 = System.currentTimeMillis() - t1;
    System.out.println(numTrees+" built in "+t1);
  }

  /** Adds trees from given forest to this forest. */
  public void addTrees(RandomForest from) {
    treesUnderConstruction_ = new RandomTree[trees_.length+from.trees_.length];
    System.arraycopy(trees_,0,treesUnderConstruction_,0,trees_.length);
    System.arraycopy(from.trees_,0,treesUnderConstruction_,trees_.length,from.trees_.length);
    trees_ = treesUnderConstruction_;
  }
  
  /** Classifies a single row using the forrest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (RandomTree tree: trees_)
      votes[tree.classify(r)] += 1;
    return Utils.maxIndex(votes,rnd);
  }
  
  /** Simply returns the miss ratio. */
  public double score(Data d) {
    int misses = 0;
    for (Row r: d)
      if (classify(r) != r.classOf)
        ++misses;
    return misses/((double)d.rows());
  }

  /** Thread that can build trees. */
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
        treesUnderConstruction_[i] = new RandomTree();
        treesUnderConstruction_[i].compute(data);
      }
    }
  }
}
