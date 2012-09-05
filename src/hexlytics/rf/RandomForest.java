package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Tree.INode;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import test.KVTest;
import water.DKV;
import water.H2O;
import water.Key;
import water.ValueArray;
import water.parser.ParseDataset;

/**
 * @author peta
 */
public class RandomForest {
  public static void build(DataAdapter dapt, double sampleRatio, int features, int trees, int maxTreeDepth, double minErrorRate) {
    build(dapt,sampleRatio,features,trees,maxTreeDepth,minErrorRate,false);
  }


  public static void build(DataAdapter dapt, double sampleRatio, int features, int trees, int maxTreeDepth,  double minErrorRate, boolean gini) {
    if (maxTreeDepth != -1) Tree.MAX_TREE_DEPTH = maxTreeDepth;  
    if (minErrorRate != -1) Tree.MIN_ERROR_RATE = minErrorRate;    
    Data d = Data.make(dapt);
    Data t = d.sampleWithReplacement(sampleRatio);
    Data v = t.complement();
    DataAdapter.FEATURES = features;
    new LocalBuilder(t,v,trees,gini);
  }
  
  private static final int NUMTHREADS = Runtime.getRuntime().availableProcessors();
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
  public void buildGini() { while (!done()) buildGini0(); }
  
  private void build0() {
    long t = System.currentTimeMillis();     
    RFTask._ = new RFTask[NUMTHREADS];
    for(int i=0;i<NUMTHREADS;i++) RFTask._[i] = new RFTask(data_);
    Statistic s = new Statistic(data_, null);
    for (Row r : data_) s.add(r);
    Tree tree = new Tree();
    RFTask._[0].put(new Job(tree, null, 0, data_, s));
    for (Thread b : RFTask._) b.start();
    for (Thread b : RFTask._)  try { b.join();} catch (InterruptedException e) { }
    tree.time_ = System.currentTimeMillis()-t;
    add(tree);
  }
  
  private void buildGini0() {
    long t = System.currentTimeMillis();     
    RFGiniTask._ = new RFGiniTask[NUMTHREADS];
    for(int i=0;i<NUMTHREADS;i++) RFGiniTask._[i] = new RFGiniTask(data_);
    GiniStatistic s = new GiniStatistic(data_, null);
    for (Row r : data_) s.add(r);
    Tree tree = new Tree();
    RFGiniTask._[0].put(new GiniJob(tree, null, 0, data_, s));
    for (Thread b : RFGiniTask._) b.start();
    for (Thread b : RFGiniTask._)  try { b.join();} catch (InterruptedException e) { }
    tree.time_ = System.currentTimeMillis()-t;
    add(tree);
  }
  
  // Dataset launched from web interface
  public static void web_main( ValueArray ary, int ntrees, int cutDepth, double cutRate, boolean useGini) {
    final int rowsize = ary.row_size();
    final int num_cols = ary.num_cols();
    String[] names = ary.col_names();
    DataAdapter dapt = null;
    double[] ds = new double[num_cols];
    final long num_chks = ary.chunks();
    for( long i=0; i<num_chks; i++ ) { // By chunks
      byte[] bits = DKV.get(ary.chunk_get(i)).get();
      final int rows = bits.length/rowsize;
      dapt = new DataAdapter(ary._key.toString(), names, 
                              names[num_cols-1], // Assume class is the last column
                              rows);
      for( int j=0; j< rows; j++ ) { // For all rows in this chunk
        for( int k=0; k<num_cols; k++ )
          ds[k] = ary.datad(bits,j,rowsize,k);
        dapt.addRow(ds);
      }
    }
    dapt.shrinkWrap();
    build(dapt, .666, -1, ntrees, cutDepth, cutRate, useGini);
  }
  
  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});    
    if(args.length==0) args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    Key fileKey = KVTest.load_test_file(new File(args[0]));    
    Key parsedKey = Key.make();
    ParseDataset.parse(parsedKey, DKV.get(fileKey));
    ValueArray va = (ValueArray) DKV.get(parsedKey);        
    DKV.remove(fileKey); // clean up and burn
    web_main(va, 100, 100, .0001, false);
  }
  
  
  /** Classifies a single row using the forest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (Tree tree : trees_) votes[tree.classify(r)] += 1;
    return Utils.maxIndex(votes, data_.random());
  }
  private int[][] scores_;
  private long errors_ = -1;
  private int[][] _confusion;
  public synchronized double validate(Tree t) {
    if (scores_ == null)  scores_ = new int[data_.rows()][data_.classes()];
    if (_confusion == null) _confusion = new int[data_.classes()][data_.classes()];
    trees_.add(t);    
    errors_ = 0; int i = 0;
    for (Row r : data_) {
      int k = t.tree_.classify(r);
      scores_[i][k]++;
      int[] votes = scores_[i];            
      if (r.classOf() != Utils.maxIndex(votes, data_.random()))  ++errors_;
      ++i;
    }
    return errors_ / (double) data_.rows();
  }
  
  
  private String pad(String s, int l) {
    String p="";
    for (int i=0;i < l - s.length(); i++) p+= " ";
    return " "+p+s;
  }
  public String confusionMatrix() {
    int error = 0;
    final int K = data_.classes()+1; 
    for (Row r : data_){
      int realClass = r.classOf();
      int[] predictedClasses = new int[data_.classes()];
      for (Tree t: trees_) {
        int k = t.tree_.classify(r);
        predictedClasses[k]++;
      }
      int predClass = Utils.maxIndexInt(predictedClasses, data_.random());
      _confusion[realClass][predClass]++;
      if (predClass != realClass) error++;
    }
    double[] e2c = new double[data_.classes()];
    for(int i=0;i<data_.classes();i++) {
      int err = -_confusion[i][i];;
      for(int j=0;j<data_.classes();j++) err+=_confusion[i][j];
      e2c[i]= Math.round((err/(double)(err+_confusion[i][i]) ) * 100) / (double) 100  ;
    }
    String [][] cms = new String[K][K+1];
  //  String [] cn = data_.data_.columnNames();
    cms[0][0] = "";
    for (int i=1;i<K;i++) cms[0][i] = ""+ (i-1); //cn[i-1];
    cms[0][K]= "err/class";
    for (int j=1;j<K;j++) cms[j][0] = ""+ (j-1); //cn[j-1];
    for (int j=1;j<K;j++) cms[j][K] = ""+ e2c[j-1];
    for (int i=1;i<K;i++) 
      for (int j=1;j<K;j++) cms[j][i] = ""+_confusion[j-1][i-1];
    int maxlen = 0;
    for (int i=0;i<K;i++) 
      for (int j=0;j<K+1;j++) maxlen = Math.max(maxlen, cms[i][j].length());
    for (int i=0;i<K;i++) 
      for (int j=0;j<K+1;j++) cms[i][j] = pad(cms[i][j],maxlen);
    String s = "";
    for (int i=0;i<K;i++) {
      for (int j=0;j<K+1;j++) s += cms[i][j];
      s+="\n";
    }
    //s+= error/(double)data_.rows();
    return s;      
  }
  
  public final synchronized long errors() { if(errors_==-1) throw new Error("unitialized errors"); else return errors_; }
}
