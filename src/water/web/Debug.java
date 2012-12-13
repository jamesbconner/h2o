
package water.web;

import hex.rf.*;

import java.util.Properties;

import water.*;
import water.ValueArray.Column;

import com.google.gson.JsonObject;

/**
 *
 * @author peta
 */
public class Debug extends JSONPage {

  @Override public String[] requiredArguments() {
    return new String[] { "action" };
  }


  public static JsonObject profileRandomForest(Properties p) throws Exception {
    long timeStart = System.currentTimeMillis();
    ValueArray ary = ServletUtil.check_array(p, RandomForestPage.DATA_KEY);
    int ntree = getAsNumber(p,RandomForestPage.NUM_TREE, 50);
    if( ntree <= 0 )
      throw new InvalidInputException("Number of trees "+ntree+" must be positive.");
    int depth = getAsNumber(p,RandomForestPage.MAX_DEPTH, Integer.MAX_VALUE);
    int binLimit = getAsNumber(p,RandomForestPage.BIN_LIMIT, 1024);
    int smp = getAsNumber(p,RandomForestPage.SAMPLE, 67);
    if( smp <= 0 || smp > 100 )
      throw new InvalidInputException("Sampling percent of "+smp+" has to be between 0 and 100");
    float sample = smp==0 ? 1.00f : (smp/100.0f);
    int gini = getAsNumber(p, RandomForestPage.GINI, Tree.StatType.GINI.ordinal());
    long seed = getAsNumber(p, RandomForestPage.RAND_SEED, 181247619891L);
    int par = getAsNumber(p, RandomForestPage.PARALLEL, 1);
    if( !(par == 0 || par == 1) )
      throw new InvalidInputException("Parallel tree building "+par+" must be either 0 or 1");
    boolean parallel =  par== 1;
    Tree.StatType statType = Tree.StatType.values()[gini];

    // Optionally, save the model
    Key modelKey = null;
    String skey = p.getProperty(RandomForestPage.MODEL_KEY, "model");
    if( skey.isEmpty() ) skey = "model";
    try {
      modelKey = Key.make(skey);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }

    int features = getAsNumber(p,RandomForestPage.FEATURES,-1);
    if ((features!=-1) && ((features<=0) || (features>=ary.num_cols() - 1)))
      throw new PageError("Number of features can only be between 1 and num_cols - 1");

    // Pick the column to classify
    int classcol = ary.num_cols()-1; // Default to the last column
    String clz = p.getProperty(RandomForestPage.CLASS_COL);
    if( clz != null ) {
      int[] clarr = RandomForestPage.parseVariableExpression(ary, clz);
      if( clarr.length != 1 )
        throw new InvalidInputException("Class has to refer to exactly one column!");
      classcol = clarr[0];
      if( classcol < 0 || classcol >= ary.num_cols() )
        throw new InvalidInputException("Class out of range");
    }
    double[] classWt = RandomForestPage.determineClassWeights(p.getProperty("classWt",""), ary, classcol, RandomForestPage.MAX_CLASSES);

    // Pick columns to ignore
    String igz = p.getProperty(RandomForestPage.IGNORE_COL);
    if( igz!=null ) System.out.println("[RF] ignoring: " + igz);
    System.out.println("[RF] class column: " + classcol);
    int[] ignores =  igz == null ? new int[0] : parseVariableExpression(ary, igz);

    if( ignores.length + 1 >= ary.num_cols() )
      throw new InvalidInputException("Cannot ignore every column");

    // Remove any prior model; about to overwrite it
    UKV.remove(modelKey);
    for( int i=0; i<=ntree; i++ ) { // Also, all related Confusions
      UKV.remove(Confusion.keyFor(modelKey,i,ary._key,classcol,true ));
      UKV.remove(Confusion.keyFor(modelKey,i,ary._key,classcol,false));
    }

    // Start the distributed Random Forest
    long startTrees = System.currentTimeMillis();
    DRF drf = hex.rf.DRF.web_main(ary,ntree,depth, sample, (short)binLimit, statType,seed, classcol,ignores,modelKey,parallel,classWt,features,false,null);
    // Output a model with zero trees (so far).
    Column c = ary._cols[classcol];
    final int classes = (int) (c._max - c._min + 1);
    Model model = new Model(modelKey,drf._treeskey,ary.num_cols(),classes,sample,ary._key,ignores, drf._features);
    // Save it to the cloud
    UKV.put(modelKey,model);
    // wait for the computation to finish
    drf.get();
    long endTrees = System.currentTimeMillis();

    // now compute the confusion matrix
    // Compute Out of Bag Error Estimate
    boolean oobee = getBoolean(p,RFView.OOBEE);
    model = UKV.get(modelKey, new Model());
    Confusion confusion = Confusion.make( model, ary._key, classcol, ignores, classWt, oobee );
    long end = System.currentTimeMillis();
    JsonObject result = new JsonObject();
    result.addProperty("preprocess",String.valueOf(startTrees - timeStart));
    result.addProperty("trees",String.valueOf(endTrees - startTrees));
    result.addProperty("confusion",String.valueOf(end - endTrees));
    return result;
  }


  @Override public JsonObject serverJson(Server server, Properties args, String sessionID) throws PageError {
    JsonObject result = new JsonObject();
    String action = args.getProperty("action");
    try {
      //
      if (action.equals("freeMem")) {
        Value v = DKV.get(Key.make(args.getProperty("key","__NOKEY__")));
        if (v == null)
          throw new Exception("Given key not found");
        if (!v.is_persisted())
          throw new Exception("Value is not persistent. Cannot be freed mem");
        v.free_mem();
      } else if (action.equals("RF")) {
        return profileRandomForest(args);
      } else {
        throw new Exception("Action "+action+" not recognized by the debug interface.");
      }
    } catch (Exception e) {
      result.addProperty("Error",e.getMessage());
    }
    return result;
  }


}
