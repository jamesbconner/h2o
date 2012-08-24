package hexlytics;

import hexlytics.RFBuilder.Director;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;

import java.util.ArrayList;

/**
 * @author peta
 */
public class RandomForest {

  private static final int numThreads = 1;
  public ArrayList<Tree> trees_ = new ArrayList<Tree>();
  int numTrees_;
  Director glue_;
  private Data data_;

  public RandomForest(Data d, Director g, int trees) { data_ = d; glue_ = g; numTrees_ = trees;  }

  public synchronized void add(Tree t) {
    if (done()) return;
    glue_.onTreeBuilt(t);  trees_.add(t);
  }

  public synchronized void addAll(ArrayList<Tree> ts) { trees_.addAll(ts); }

  public synchronized ArrayList<Tree> trees() { return trees_; }

  synchronized boolean done() { return trees_.size() >= numTrees_; }

  public void terminate() {  numTrees_ = 0; }

  public void build() {
    ArrayList<Thread> bees = new ArrayList<Thread>();
    for (int i = 0; i < numThreads; i++)
      bees.add(new Thread() {
        public void run() {
          while (!done())  add(new Tree().compute(data_));
        }
      });
    for (Thread b : bees) b.start();
    for (Thread b : bees)  try { b.join();} catch (InterruptedException e) { }
  }

  /** Classifies a single row using the forest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (Tree tree : trees_) votes[tree.classify(r)] += 1;
    return Utils.maxIndex(votes, data_.random());
  }

  private int[][] scores_;
  private long errors_ = -1;
  
  public synchronized double validate(Tree t) {
    if (scores_ == null)  scores_ = new int[data_.rows()][data_.classes()];
    trees_.add(t);    
    errors_ = 0;
    int i = 0;
    for (Row r : data_) {
      scores_[i][t.tree_.classify(r.v())]++;
      int[] votes = scores_[i];            
      if (r.classOf() != Utils.maxIndex(votes, data_.random()))  ++errors_;
      ++i;
    }
    return errors_ / (double) data_.rows();
  }
  
  public final synchronized long errors() {
    if(errors_ == -1) throw new Error("unitialized errors"); else return errors_;
    }

}
