package hexlytics;

import hexlytics.data.Data;
import hexlytics.data.Data.Row;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author peta
 */
public class RandomForest {
  
  private static final int numThreads = 4;
  private static Random rnd = new Random();  
  public ArrayList<Tree> trees_ = new ArrayList<Tree>();
  int numberOfTrees_;
  private Data data_;
  
  public RandomForest(Data d, int trees) { data_ = d; numberOfTrees_ = trees; }  

  public synchronized void add(Tree t) { if(!done()) trees_.add(t); }
  public synchronized void addAll(ArrayList<Tree> ts) { trees_.addAll(ts); }
  public synchronized ArrayList<Tree> trees() { return trees_; }
  synchronized boolean done() { return trees_.size() >= numberOfTrees_; }
  public void terminate() { numberOfTrees_ =0; }
    
  public void build() {
    ArrayList<Thread> bees = new ArrayList<Thread>();
    for(int i=0;i<numThreads;i++) 
      bees.add(new Thread() {       
        public void run() {
          while(!done()) add(new Tree().compute(data_));
        }
    });
    for(Thread b : bees) b.start();
    for(Thread b : bees)  try{ b.join(); }catch( InterruptedException e ){ }
  }
  
  /** Classifies a single row using the forrest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (Tree tree: trees_)
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

}
