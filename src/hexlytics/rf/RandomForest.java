package hexlytics.rf;

import hexlytics.rf.Data.Row;
import java.io.File;
import java.util.concurrent.ExecutionException;
import test.TestUtil;
import water.*;

public class RandomForest {
  final Tree[] _trees;          // The trees that got built
  final Data _data;             // The data to train on.
  final Data _validate;         // The data to validate on.  NULL if validating on other data.

  // Build N trees via the Random Forest algorithm.
  public RandomForest( DRF drf, DataAdapter dapt, double sampleRatio, int ntrees, int maxTreeDepth, double minErrorRate, Tree.StatType stat ) {
    Data d = Data.make(dapt);
    // Training data.  For now: all of it.
    // TODO: if the training data fits in this Node, then we need to do sampling.
    _data = d; // d.sampleWithReplacement(sampleRatio);
    //_validate = _data.complement();
    _validate = null;
    _trees = new Tree[ntrees];
    for( int i=0; i<ntrees; i++ )
      H2O.FJP_NORM.execute(_trees[i] = new Tree(d,maxTreeDepth,minErrorRate,stat));
    // Block until all trees are built
    try {
      for( int i=0; i<ntrees; i++ ) {
        _trees[i].get();        // Block for a tree
        // Atomic-append to the list of trees
        new AppendKey(_trees[i].toKey()).fork(drf._treeskey);
      }
    } catch( InterruptedException e ) {
      // Interrupted after partial build?
    } catch( ExecutionException e ) {
    }
  }

  /*private void buildGini0() {
    long t = System.currentTimeMillis();
    RFGiniTask._ = new RFGiniTask[NUMTHREADS];
    for(int i=0;i<NUMTHREADS;i++)
      RFGiniTask._[i] = new RFGiniTask(data_);
    RFGiniTask task = RFGiniTask._[0];
    task.stats_[0].reset(data_);
    for (Row r : data_) task.stats_[0].add(r);
    GiniStatistic.Split s = task.stats_[0].split();
    Tree tree = new Tree();
    if (s.isLeafNode()) {
      tree.tree_ = new LeafNode(0,s.split);
    } else {
      RFGiniTask._[0].put(new GiniJob(tree, null, 0, data_, s));
      for (Thread b : RFGiniTask._) b.start();
      for (Thread b : RFGiniTask._)  try { b.join();} catch (InterruptedException e) { }
    }
    tree.time_ = System.currentTimeMillis()-t;
    add(tree);
  } */

  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});
    if(args.length==0) args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    Key fileKey = TestUtil.load_test_file(new File(args[0]));
    ValueArray va = TestUtil.parse_test_key(fileKey);
    DKV.remove(fileKey); // clean up and burn
    DRF.web_main(va, 10, 100, .15, true);
    System.out.println("done");
    System.exit(-1); // I hope this sysexit is ok:)
  }


  /** Classifies a single row using the forest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (Tree tree : _trees) votes[tree.classify(r)] += 1;
    return Utils.maxIndex(votes, _data.random());
  }
  private int[][] scores_;
  private long errors_ = -1;
  private int[][] _confusion;
  public synchronized double validate(Tree t) {
    if (scores_ == null)  scores_ = new int[_data.rows()][_data.classes()];
    if (_confusion == null) _confusion = new int[_data.classes()][_data.classes()];
    errors_ = 0; int i = 0;
    for (Row r : _data) {
      int k = t._tree.classify(r);
      scores_[i][k]++;
      int[] votes = scores_[i];
      if (r.classOf() != Utils.maxIndex(votes, _data.random()))  ++errors_;
      ++i;
    }
    return errors_ / (double) _data.rows();
  }


  private String pad(String s, int l) {
    String p="";
    for (int i=0;i < l - s.length(); i++) p+= " ";
    return " "+p+s;
  }
  public String confusionMatrix() {
    int error = 0;
    final int K = _data.classes()+1;
    for (Row r : _data){
      int realClass = r.classOf();
      int[] predictedClasses = new int[_data.classes()];
      for (Tree t: _trees) {
        int k = t._tree.classify(r);
        predictedClasses[k]++;
      }
      int predClass = Utils.maxIndexInt(predictedClasses, _data.random());
      _confusion[realClass][predClass]++;
      if (predClass != realClass) error++;
    }
    double[] e2c = new double[_data.classes()];
    for(int i=0;i<_data.classes();i++) {
      int err = -_confusion[i][i];;
      for(int j=0;j<_data.classes();j++) err+=_confusion[i][j];
      e2c[i]= Math.round((err/(double)(err+_confusion[i][i]) ) * 100) / (double) 100  ;
    }
    String [][] cms = new String[K][K+1];
  //  String [] cn = _data._data.columnNames();
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
    //s+= error/(double)_data.rows();
    return s;
  }

  public final synchronized long errors() { if(errors_==-1) throw new Error("unitialized errors"); else return errors_; }
}
