package hexlytics;

import hexlytics.RFBuilder.BuilderGlue;
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
  BuilderGlue glue_;
  private Data data_;
  
  public RandomForest(Data d, BuilderGlue g, int trees) { data_ = d; glue_ = g; numTrees_ = trees; }  

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
    glue_.onBuilderTerminated();
  }
  
  /** Classifies a single row using the forest. */
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
