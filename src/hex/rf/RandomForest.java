package hex.rf;

import hex.rf.Tree.StatType;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

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

  public RandomForest( DRF drf, Data data, int ntrees, int maxTreeDepth, double minErrorRate, StatType stat, boolean singlethreaded ) {

    // Build N trees via the Random Forest algorithm.
    _data = data;
    long start = System.currentTimeMillis();
    try { // build one tree at a time, and forget it
      for( int i=0; i<ntrees; i++ ) {
        Tree t = null;
        H2O.FJP_NORM.submit(t = new Tree(_data.clone(),maxTreeDepth,minErrorRate,stat,features(), i + data.seed()));
        t.get();        // Block for a tree
        new AppendKey(t.toKey()).fork(drf._treeskey);        // Atomic-append to the list of trees
        long now = System.currentTimeMillis();
        System.out.println("Tree "+i+" ready after "+(now-start)+" msec");
      }
    } catch( InterruptedException e ) { // Interrupted after partial build?
    } catch( ExecutionException e ) { throw new Error(e); }
  }

  public static class OptArgs extends Arguments.Opt {
	String file = "smalldata/poker/poker-hand-testing.data";
	String rawKey;
	String parsdedKey;
	String validationFile = null;
	String h2oArgs = "";
	int ntrees = 10;
	int depth = Integer.MAX_VALUE;
	double cutRate = 0;
	String statType = "entropy";
	int seed = 42;
	boolean singlethreaded;
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
    System.out.println("H2O args = " + Arrays.toString(h2oArgs));
    H2O.main(h2oArgs);
    System.out.println(ARGS.file);
    Thread.sleep(100);
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
    int ntrees = ARGS.ntrees;
    if(ntrees == 0) {
      System.out.println("Nothing to do as ntrees == 0");
      UDPRebooted.global_kill(2);
      return;
    }
    DRF.sample = (ARGS.validationFile == null || ARGS.validationFile.isEmpty());
    DRF.forceNoSample = (ARGS.validationFile != null && !ARGS.validationFile.isEmpty());
    StatType st = ARGS.statType.equals("gini") ? StatType.GINI : StatType.ENTROPY;
    Utils.pln("[RF] Starting RF.");
    final int num_cols = va.num_cols();
    final int classcol = num_cols-1; // Defaults to last column
    long t1 = System.currentTimeMillis();
    DRF drf = DRF.web_main(va, ARGS.ntrees, ARGS.depth, ARGS.cutRate, st, ARGS.seed, ARGS.singlethreaded, classcol);

    final int classes = (short)((va.col_max(classcol) - va.col_min(classcol))+1);
    Model model = new Model(null,drf._treeskey,num_cols,classes);

    Key[] tkeys = null;
    while(tkeys == null || tkeys.length!=ntrees) tkeys = drf._treeskey.flatten();
    long t2 = System.currentTimeMillis();
    assert tkeys.length == ntrees;
    if(ARGS.validationFile != null && !ARGS.validationFile.isEmpty()){ // validate n the suplied file
      DRF.forceNoSample = true;
      Key valKey = KeyUtil.load_test_file(ARGS.validationFile);
      ValueArray valAry = KeyUtil.parse_test_key(valKey);
      Key[] keys = new Key[(int)valAry.chunks()];
      for( int i=0; i<keys.length; i++ )
        keys[i] = valAry.chunk_get(i);
      Confusion c = Confusion.make( model, valKey, classcol);
      c.mapAll();
      c.report();
    } else {
      Confusion c = Confusion.make( model, drf._arykey, classcol);
      c.setValidation(drf._validation.getPermutationArray());
      c.mapAll();
      c.report();
    }
    System.out.println("Random forest finished in: " + (t2 - t1) + " ms");
    UDPRebooted.global_kill(2);
  }
}


