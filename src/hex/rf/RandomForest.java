package hex.rf;
import hex.rf.Tree.StatType;

import java.io.File;

import water.*;
import water.util.KeyUtil;

/**
 * A RandomForest can be used for growing or validation. The former starts with a known target number of trees,
 * the latter is incrementally populated with trees as they are built.
 * Validation and error reporting is not supported when growing a forest.
 */
public class RandomForest {
  final Data _data;             // The data to train on.
  private int _features = -1;   // features to check at each split

  public RandomForest(DRF drf, Data data, int ntrees, int maxTreeDepth, double minErrorRate, StatType stat, boolean parallelTrees) {
    // Build N trees via the Random Forest algorithm.
    _data = data;
    Utils.startTimer("alltrees");
    Tree[] trees = new Tree[ntrees];
    for (int i = 0; i < ntrees; ++i) {
      trees[i] = new Tree(_data,maxTreeDepth,minErrorRate,stat,features(), i+data.seed(), drf._treeskey, drf._modelKey,i,drf._ntrees, drf._sample);
      if (!parallelTrees) water.DRemoteTask.invokeAll(new Tree[]{trees[i]});
    }
    if (parallelTrees) water.DRemoteTask.invokeAll(trees);

    Utils.pln("All trees ("+ntrees+") done in "+ Utils.printTimer("alltrees"));
  }

  public static class OptArgs extends Arguments.Opt {
	String file = "smalldata/poker/poker-hand-testing.data";
	String rawKey;
	String parsdedKey;
	String validationFile = null;
	String h2oArgs = " --name=Test"+ System.nanoTime()+ " ";
	int ntrees = 10;
	int depth = Integer.MAX_VALUE;
	int sample = 67;
	int binLimit = 1024;
	int classcol = -1;
	int features = -1;
	String statType = "entropy";
	int seed = 42;
  }

  static final OptArgs ARGS = new OptArgs();

  public int features() { return _features== -1 ? (int)Math.sqrt(_data.columns()) : _features; }


  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments(args);
    arguments.extract(ARGS);
    if(ARGS.h2oArgs.startsWith("\"") && ARGS.h2oArgs.endsWith("\""))
      ARGS.h2oArgs = ARGS.h2oArgs.substring(1, ARGS.h2oArgs.length()-1);
    ARGS.h2oArgs = ARGS.h2oArgs.trim();
    String [] h2oArgs = ARGS.h2oArgs.split("[ \t]+");
    H2O.main(h2oArgs);
    ValueArray va;
    // get the input data
    if(ARGS.parsdedKey != null) // data already parsed
      va = (ValueArray)DKV.get(Key.make(ARGS.parsdedKey));
    else if(ARGS.rawKey != null) // data loaded in K/V, not parsed yet
      va = KeyUtil.parse_test_key(Key.make(ARGS.rawKey),Key.make(KeyUtil.getHexKeyFromRawKey(ARGS.rawKey)));
    else { // data outside of H2O, load and parse
      File f = new File(ARGS.file);
      System.out.println("[RF] Loading file " + f);
      Key fk = KeyUtil.load_test_file(f);
      va = KeyUtil.parse_test_key(fk,Key.make(KeyUtil.getHexKeyFromFile(f)));
      DKV.remove(fk);
    }
    if(ARGS.ntrees == 0) {
      System.out.println("Nothing to do as ntrees == 0");
      UDPRebooted.global_kill(2);
      return;
    }
    StatType st = ARGS.statType.equals("gini") ? StatType.GINI : StatType.ENTROPY;
    final int num_cols = va.num_cols();
    final int classcol = ARGS.classcol == -1 ? num_cols-1: ARGS.classcol; // Defaults to last column
    assert ARGS.sample >0 && ARGS.sample<=100;
    assert ARGS.ntrees >=0;
    assert ARGS.binLimit > 0 && ARGS.binLimit <= Short.MAX_VALUE;
    DRF drf = DRF.web_main(va, ARGS.ntrees, ARGS.depth,  ((float)ARGS.sample/100.0f), (short)ARGS.binLimit, st, ARGS.seed, classcol, new int[0], Key.make("model"),true);
    drf.get(); // block

    Model model = UKV.get(drf._modelKey, new Model());

    String t2 =Utils.printTimer("maintimer");
    Utils.pln("[RF] trees done in "+ t2);
    Utils.startTimer("validation");


    if(ARGS.validationFile != null && !ARGS.validationFile.isEmpty()){ // validate n the suplied file
      Key valKey = KeyUtil.load_test_file(ARGS.validationFile);
      ValueArray valAry = KeyUtil.parse_test_key(valKey);
      Key[] keys = new Key[(int)valAry.chunks()];
      for( int i=0; i<keys.length; i++ )
        keys[i] = valAry.chunk_get(i);
      Confusion c = Confusion.make( model, valKey, classcol);
      c.report();
    } else {
      Confusion c = Confusion.make( model, drf._arykey, classcol);
      c.setValidation(drf._validation.getPermutationArray());
      c.report();
    }
    Utils.pln("[RF] Random forest finished in: " + t2);
    Utils.pln("[RF] Validation done in: " + Utils.printTimer("validation"));
    UDPRebooted.global_kill(2);
  }
}
