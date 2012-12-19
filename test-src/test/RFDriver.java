package test;
import water.*;
import hex.rf.*;
import hex.rf.Tree.StatType;
import java.io.File;

// Drive Random Forest directly
class RFDriver {
  public static class OptArgs extends Arguments.Opt {
	String file = "smalldata/poker/poker-hand-testing.data";
	String rawKey;
	String parsdedKey;
	String validationFile;
	String h2oArgs = " --name=Test"+ System.nanoTime()+ " ";
	int ntrees = 10;
	int depth = Integer.MAX_VALUE;
	int sample = 67;
	int binLimit = 1024;
	int classcol = -1;
	int features = -1;
	int parallel = 1;
	boolean outOfBagError;
	boolean stratify;
	String strata;
	String statType = "entropy";
	int seed = 42;
	String ignores;
	int nnodes = 1;
	int cloudFormationTimeout=10; // wait for up to 10seconds
  }

  static final OptArgs ARGS = new OptArgs();

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
      va = ValueArray.value(DKV.get(Key.make(ARGS.parsdedKey)));
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
      UDPRebooted.T.shutdown.broadcast();
      return;
    }
    StatType st = ARGS.statType.equals("gini") ? StatType.GINI : StatType.ENTROPY;
    int[] ignores = new int[0];
    if (ARGS.ignores!=null) {
      String[] strs = ARGS.ignores.split(",");
      ignores = new int[strs.length];
      for(int i=0;i<ignores.length;i++)
        ignores[i] = Integer.parseInt(strs[i]);
    }

    final int num_cols = va._cols.length;
    final int classcol = ARGS.classcol == -1 ? num_cols-1: ARGS.classcol; // Defaults to last column
    assert ARGS.sample >0 && ARGS.sample<=100;
    assert ARGS.ntrees >=0;
    assert ARGS.binLimit > 0 && ARGS.binLimit <= Short.MAX_VALUE;
    DRF drf = DRF.web_main(va, ARGS.ntrees, ARGS.depth,  (ARGS.sample/100.0f), (short)ARGS.binLimit, st, ARGS.seed, classcol, ignores, Key.make("model"),ARGS.parallel==1, null,/*features*/-1, false, null);
    drf.get(); // block
    Model model = UKV.get(drf._modelKey, new Model());
    Utils.pln("[RF] Random forest finished in "+ drf._t_main);

    Timer t_valid = new Timer();
    Key valKey = drf._arykey;
    Utils.pln("[RF] Computing out of bag error");
    Confusion.make( model, valKey, classcol,ignores, null, true).report();

    if(ARGS.validationFile != null && !ARGS.validationFile.isEmpty()){ // validate on the supplied file
      File f = new File(ARGS.validationFile);
      System.out.println("[RF] Loading validation file " + f);
      Key fk = KeyUtil.load_test_file(f);
      ValueArray v = KeyUtil.parse_test_key(fk,Key.make(KeyUtil.getHexKeyFromFile(f)));
      valKey = v._key;
      DKV.remove(fk);
      Confusion.make( model, valKey, classcol,ignores, null, false).report();
    }
    Utils.pln("[RF] Validation done in: " + t_valid);
    UDPRebooted.T.shutdown.broadcast();
  }
}
