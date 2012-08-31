 package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Tree.INode;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author peta
 */
public class RandomForest {

  public static void build(DataAdapter dapt, double sampleRatio, int features, int trees, int maxTreeDepth,  double minErrorRate, int threads) {
    if (maxTreeDepth != -1) Tree.MAX_TREE_DEPTH = maxTreeDepth;  
    if (minErrorRate != -1) Tree.MIN_ERROR_RATE = minErrorRate;

    
    Data d = Data.make(dapt);
    Data t = d.sampleWithReplacement(sampleRatio);
    Data v = t.complement();
    DataAdapter.FEATURES = features;
    numThreads = threads;
    new LocalBuilder(t,v,trees);
  }
  
  private static int numThreads = -1;
  public ArrayList<Tree> trees_ = new ArrayList<Tree>();
  private int numTrees_;
  private Director glue_;
  private Data data_;

  public RandomForest(Data d, Director g, int trees) { data_ = d; glue_ = g; numTrees_ = trees;  }

  private synchronized void add(Tree t) { if (done()) return; glue_.onTreeBuilt(t); trees_.add(t); }
  public synchronized void addAll(ArrayList<Tree> ts) { trees_.addAll(ts); }
  public synchronized ArrayList<Tree> trees() { return trees_; }
  synchronized boolean done() { return trees_.size() >= numTrees_; }
  public void terminate() {  numTrees_ = 0; }
  public void build() {  while (!done()) build0();  }
  
  private void build0() {
    long t = System.currentTimeMillis();     
    if (numThreads == -1) numThreads =  Runtime.getRuntime().availableProcessors();
    RFTask._ = new RFTask[numThreads];
    for(int i=0;i<numThreads;i++) RFTask._[i] = new RFTask(data_);
    Statistic s = new Statistic(data_, null);
    for (Row r : data_) s.add(r);
    Tree tree = new Tree();
    RFTask._[0].put(new Job(tree, null, 0, data_, s));
    for (Thread b : RFTask._) b.start();
    for (Thread b : RFTask._)  try { b.join();} catch (InterruptedException e) { }
    tree.time_ = System.currentTimeMillis()-t;
    add(tree);
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
    errors_ = 0; int i = 0;
    for (Row r : data_) { 
      scores_[i][t.tree_.classify(r)]++;
      int[] votes = scores_[i];            
      if (r.classOf() != Utils.maxIndex(votes, data_.random()))  ++errors_;
      ++i;
    }
    return errors_ / (double) data_.rows();
  }
  
  public final synchronized long errors() { if(errors_==-1) throw new Error("unitialized errors"); else return errors_; }
}

class Job {
  final INode _node; final int _direction; final Data _data;
  final Statistic _stat; final Tree _tree;
  Job(Tree t, INode n, int i, Data d, Statistic s) {
    _tree=t; _node=n; _direction=i; _data=d; _stat=s;
  }
  void run() { 
    Job[] jobs = new Job[2];
    INode newnode =  _tree.compute(_node == null ? 0 : _node.nodeDepth_+1,_data, _stat, jobs);
    if (_node==null)  _tree.tree_ = newnode;
    else _node.set(_direction,newnode);
    RFTask task = (RFTask) Thread.currentThread();
    if (jobs[0]==null) return;
    task.put(jobs[0]); task.put(jobs[1]);
  }
}

class RFTask extends Thread {
  static RFTask[] _;  
  static Data _data;
  static Data data() { return _data; }
  RFTask(Data d){ _data=d; }
  Queue<Job> _q = new LinkedList<Job>(); 
  boolean idle = false;  
  public void run() {
    while (true) {
      Job j = take();
      if (j==null) for (RFTask r : _) if ( (j = r.take()) != null) break;
      if (j!=null) j.run();
      else if(idle()) return;
    }
  }
  boolean idle() {
    idle = true;
    boolean done = true;
    for (RFTask r : _) done &= r.idle;
    if (done) return true;
    try {
      sleep(100);
      } catch (Exception _){ }
    idle = false;
    return false;
  }
  synchronized void put(Job j){ if (j!=null) _q.add(j); }
  synchronized Job take() { return _q.isEmpty()? null : _q.remove(); }
}
