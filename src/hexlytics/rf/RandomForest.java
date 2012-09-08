package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Utils.MinMaxAvg;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import test.TestUtil;
import water.AppendKey;
import water.DKV;
import water.H2O;
import water.Key;
import water.ValueArray;

public class RandomForest {
  final ArrayList<Tree> _trees = new ArrayList<Tree>();  // The trees that got built
  final int _ntrees;   // The target number of trees to make       
  final Data _data;    
  
  public RandomForest( DRF drf, DataAdapter dapt, double sampleRatio, int ntrees, int maxTreeDepth, 
      double minErrorRate, Tree.StatType stat) {
    this(drf,dapt,sampleRatio,ntrees,maxTreeDepth,minErrorRate,stat,true); // block by default
  }

  public RandomForest( DRF drf, DataAdapter dapt, double sampleRatio, int ntrees, int maxTreeDepth, 
      double minErrorRate, Tree.StatType stat, boolean block ) {
    this(dapt, ntrees);
    for( int i=0; i<_ntrees; i++ ) {
      _trees.add(new Tree(_data,maxTreeDepth,minErrorRate,stat));
      H2O.FJP_NORM.execute(_trees.get(i));
    }
    if (block) blockForTrees(drf);  // Block until all trees are built
    testReport();
  }
  
  public RandomForest( DataAdapter d , int ntrees ) { _data = Data.make(d); _ntrees = ntrees;  }
  
  public final void blockForTrees(DRF drf) {
    try {
      for( Tree t : _trees) {
        t.get();   // Block for a tree
        new AppendKey(t.toKey()).fork(drf._treeskey);  // Atomic-append to the list of trees
      }
    } catch( InterruptedException e ) { // Interrupted after partial build?
    } catch( ExecutionException e ) { }
  }

  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});
    if(args.length==0) args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    Key fileKey = TestUtil.load_test_file(new File(args[0]));
    ValueArray va = TestUtil.parse_test_key(fileKey);
    DKV.remove(fileKey); // clean up and burn
    Key key = DRF.web_main(va, 10, 100, .15, Tree.StatType.oldEntropy);    
    
    
    
   // r.
//    UDPRebooted.global_kill(); ... trying to kill the cloud here...
  }


  /** Classifies a single row using the forest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (Tree tree : _trees)
        votes[tree.classify(r)] += 1;
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
    if (_confusion == null) _confusion = new int[_data.classes()][_data.classes()];
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
  
  protected final double validate(int ntrees, Data data) {
    double error = 0;
    double total = 0;
    int[] votes = new int[data.classes()];
    for (Row row: data) {
      total += row.weight();
      Arrays.fill(votes,0);
      for (int i = 0; i <= ntrees; ++i)
        ++votes[_trees.get(i).classify(row)];
      if (Utils.maxIndex(votes,data.random())!=row.classOf())
        error += row.weight();
    }
    return error/total;
  }
  
  
  public final void testReport() {
    MinMaxAvg tbt = new MinMaxAvg();
    MinMaxAvg td = new MinMaxAvg();
    MinMaxAvg tl = new MinMaxAvg();
    MinMaxAvg ta = new MinMaxAvg();
    double ensembleError = 0;
    int i=0;
    for (Tree t : _trees) {
      ensembleError = validate(i,_data);
      System.out.println(i+++": "+ensembleError);
      tbt.add(t._timeToBuild);
      td.add(t._tree.depth());
      tl.add(t._tree.leaves());
      ta.add(t.validate(_data));
    }
    
    System.out.println("\n----- Random Forest finished -----\n");
    System.out.println(" Tree time to build: "+tbt);
    System.out.println(" Tree depth:         "+td);
    System.out.println(" Tree leaves:        "+tl);
    System.out.println(" Tree error rate:    "+ta);
    System.out.println("");
    System.out.println(" Data rows:          "+_data.rows());
    System.out.println(" Overall error:      "+ensembleError);
    System.out.println(confusionMatrix());  
    
  }
}
