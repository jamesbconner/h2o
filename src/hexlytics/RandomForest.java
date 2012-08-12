package hexlytics;

import hexlytics.RFBuilder.Director;
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
  int numTrees_;
  Director glue_;
  private Data data_;
  
  public RandomForest(Data d, Director g, int trees) { data_ = d; glue_ = g; numTrees_ = trees; }  

  public synchronized void add(Tree t) { if(!done()){ glue_.onTreeReady(t); trees_.add(t); } }
  public synchronized void addAll(ArrayList<Tree> ts) { trees_.addAll(ts); }
  public synchronized ArrayList<Tree> trees() { return trees_; }
  synchronized boolean done() { return trees_.size() >= numTrees_; }
  public void terminate() { numTrees_ =0; }
    
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
  
  
  /** Classifies a single row using the forest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (Tree tree: trees_)
      votes[tree.classify(r)] += 1;
    return Utils.maxIndex(votes,rnd);
  }
  
  private int[][] scores_;
  
  public double validate(Tree t) { 
    if (scores_==null) scores_ = new int[data_.rows()][data_.classes()];
    trees_.add(t);
    int right=0, wrong =0;
    for (Row r : data_) {
      scores_[r.index][t.tree_.classify(r.v)]++;            
      int[]votes = scores_[r.index];
      for(int i=0;i<data_.classes();i++) 
        if(i==r.classOf()) right+=votes[i]; else wrong+=votes[i];    
    }
    return wrong/(double)right;
  }

  
}
