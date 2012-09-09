package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Tree.StatType;
import hexlytics.rf.Utils.MinMaxAvg;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import test.TestUtil;
import water.*;

/**
 * A RandomForest can be used for growing or validation. The former starts with a known target number of trees,
 * the latter is incrementally populated with trees as they are built.  
 * Validation and error reporting is not supported when growing a forest.
 */
public class RandomForest {
  final ArrayList<Tree> _trees = new ArrayList<Tree>();  // The trees that got built
  final int _ntrees;   // The target number of trees to make
  final Data _data;
  private int _features = -1;  // features to check at each split
  private int[][] scores_;
  private long errors_ = -1;
  private int[][] _confusion;  
  final boolean _validation;

  public RandomForest( DRF drf, Data d, int ntrees, int maxTreeDepth, double minErrorRate, StatType stat) {
    this(drf,d,ntrees,maxTreeDepth,minErrorRate,stat,true); // block by default
  }

  public RandomForest( DRF drf, Data d, int ntrees, int maxTreeDepth, double minErrorRate, StatType stat, boolean block ) {
    this(d, ntrees, false);
    for( int i=0; i<_ntrees; i++ ) {
      _trees.add(new Tree(_data,maxTreeDepth,minErrorRate,stat,features()));
      H2O.FJP_NORM.execute(_trees.get(i));
    }
    if (block) blockForTrees(drf);  // Block until all trees are built
  }

  public RandomForest( Data d , int ntrees, boolean validation ) { _data = d; _ntrees = ntrees; _validation=validation; }

  public final void blockForTrees(DRF drf) {
    try { for( Tree t : _trees) {
        t.get();   // Block for a tree
        new AppendKey(t.toKey()).fork(drf._treeskey);  // Atomic-append to the list of trees
    }} catch( InterruptedException e ) { // Interrupted after partial build?
    }  catch( ExecutionException e ) { }
  }
  
  public static class OptArgs extends Arguments.Opt {
	String file = "smalldata/poker/poker-hand-testing.data";
	String h2oArgs = "";
	int ntrees = 10;
	int depth = -1;
	double cutRate = 0.15;
	String statType = "entropy";
  }
  
  static final OptArgs OPT_ARGS = new OptArgs();
  
  public static Key[] get(Key key) {
    Value val = DKV.get(key);
    if( val == null )  return new Key[0];
    byte[] bits = val.get();
    int off = 0;
    int nkeys = UDP.get4(bits,(off+=4)-4);
    Key treekeys[] = new Key[nkeys];
    for( int i=0; i<nkeys; i++ )
      off += (treekeys[i] = Key.read(bits,off)).wire_len();
    return treekeys;
  }

  public int features() { return _features== -1 ? (int)Math.sqrt(_data.columns()) : _features; }

  
  public static void main(String[] args) throws Exception {
	Arguments arguments = new Arguments(args);
	arguments.extract(OPT_ARGS);
	if(OPT_ARGS.h2oArgs.startsWith("\"") && OPT_ARGS.h2oArgs.endsWith("\""))
		OPT_ARGS.h2oArgs = OPT_ARGS.h2oArgs.substring(1, OPT_ARGS.h2oArgs.length()-1); 
	OPT_ARGS.h2oArgs = OPT_ARGS.h2oArgs.trim();
	String [] h2oArgs = OPT_ARGS.h2oArgs.split("[ \t]+");
	System.out.println("H2O args = " + Arrays.toString(h2oArgs));
	H2O.main(h2oArgs);
	System.out.println(OPT_ARGS.file);
    Key fileKey = TestUtil.load_test_file(OPT_ARGS.file);
    ValueArray va = TestUtil.parse_test_key(fileKey);
    DKV.remove(fileKey); // clean up and burn
    int ntrees = OPT_ARGS.ntrees;
    DRF.SAMPLE = true;
    Key key = DRF.web_main(va, ntrees, 100, .15, StatType.ENTROPY);

    while (get(key).length != ntrees) Thread.sleep(100);
    Key[] keys = get(key);
    Tree[] trees = new Tree[keys.length];
    RandomForest vrf = DRF._vrf;
    for(int i=0;i<trees.length;i++)
      vrf._trees.add( trees[i] = Tree.fromKey(keys[i]) );
    vrf.testReport();
    UDPRebooted.global_kill();
  }


  /** Classifies a single row using the forest. */
  public int classify(Row r) {
    int[] votes = new int[r.numClasses()];
    for (Tree tree : _trees)
        votes[tree.classify(r)] += 1;
    return Utils.maxIndex(votes, _data.random());
  }

  private int lastTreeValidated; 
  
  /** Incrementally validate new trees in the forest. */
  private void validate() {
    if (!_validation) throw new Error("Can't validate on training data.");
    if (scores_ == null)  scores_ = new int[_data.rows()][_data.classes()];
    if (_confusion == null) _confusion = new int[_data.classes()][_data.classes()];    
    errors_ = 0; int i = 0;
    for (Row r : _data) {
      int realClass = r.classOf();
      int[] predClasses = scores_[i];
      for(int j = lastTreeValidated; j< _trees.size(); j++) 
        predClasses[_trees.get(j)._tree.classify(r)]++;
      int predClass = Utils.maxIndex(predClasses, _data.random());
      if (realClass != predClass)  ++errors_;
      _confusion[realClass][predClass]++;      
      ++i;
    } 
    lastTreeValidated = _trees.size()-1;
  }

  /** The number of misclassifications observed to date. */
  private long errors() { if(errors_==-1) throw new Error("unitialized"); else return errors_; }

  
  private String pad(String s, int l) {
    String p="";
    for (int i=0;i < l - s.length(); i++) p+= " ";
    return " "+p+s;
  }
  private String confusionMatrix() {
    if (_confusion == null) validate();
    int error = 0;
    final int K = _data.classes()+1;
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
    return s;
  }


  public final void testReport() {
    validate();
    MinMaxAvg tbt = new MinMaxAvg();
    MinMaxAvg td = new MinMaxAvg();
    MinMaxAvg tl = new MinMaxAvg();
    MinMaxAvg ta = new MinMaxAvg();
    String s = "";
    for (Tree t : _trees) {
      tbt.add(t._timeToBuild);
      td.add(t._tree.depth());
      tl.add(t._tree.leaves());
      ta.add(t.validate(_data));
    }
    double err = errors()/(double) _data.rows();

    s+= "              Type of random forest: classification\n" +
        "                    Number of trees: "+ _trees.size() +"\n"+
        "No of variables tried at each split: " + features() +"\n"+
        "             Estimate of error rate: " + Math.round(err *10000)/100 + "%  ("+err+")\n"+        
        "                   Confusion matrix:\n" + confusionMatrix();
    
    System.out.println(s);
    System.out.println(" Tree time to build: "+tbt);
    System.out.println(" Tree depth:         "+td);
    System.out.println(" Tree leaves:        "+tl);
    System.out.println(" Data rows:          "+_data.rows());

  }
}
