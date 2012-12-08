
package water.api;

import com.google.gson.JsonObject;
import hex.rf.Confusion;
import hex.rf.DRF;
import hex.rf.Model;
import hex.rf.Tree;
import java.util.HashMap;
import java.util.Properties;
import water.H2O;
import water.Key;
import water.UKV;
import water.ValueArray;
import water.web.*;

/**
 *
 * @author peta
 */
public class RF extends Request {

  public static final String HTTP_KEY = "key";
  public static final String HTTP_NUM_TREES = "ntree";
  public static final String HTTP_DEPTH = "depth";
  public static final String HTTP_SAMPLE = "sample";
  public static final String HTTP_BIN_LIMIT = "bin_limit";
  public static final String HTTP_GINI = "gini";
  public static final String HTTP_SEED = "seed";
  public static final String HTTP_PARALLEL = "parallel";
  public static final String HTTP_MODEL_KEY = "model_key";
  public static final String HTTP_CLASS = "class";
  public static final String HTTP_IGNORE = "ignore";
  public static final String HTTP_OOBEE = "oobee";
  public static final String HTTP_FEATURES = "features";
  public static final String HTTP_STRATIFY = "stratify";
  public static final String HTTP_STRATA = "strata";

  protected final ExistingKeyArgument _key = new ExistingKeyArgument(HTTP_KEY,"Data key");
  protected final IntegerArgument _numTrees = new IntegerArgument(HTTP_NUM_TREES,0,Integer.MAX_VALUE,50,"Number of trees to build");
  protected final IntegerArgument _depth = new IntegerArgument(HTTP_DEPTH,0,Integer.MAX_VALUE,0,"Max tree depth");
  protected final IntegerArgument _sample = new IntegerArgument(HTTP_SAMPLE, 0, 101, 67, "Sample rate");
  protected final IntegerArgument _binLimit = new IntegerArgument(HTTP_BIN_LIMIT,0,65536, 1024, "Max # of bins");
  protected final BooleanArgument _gini = new BooleanArgument(HTTP_GINI,false,"Use Gini statistics (default is Entropy)");
  protected final IntegerArgument _seed = new IntegerArgument(HTTP_SEED,0,Integer.MAX_VALUE,null,"Random seed");
  protected final BooleanArgument _parallel = new BooleanArgument(HTTP_PARALLEL,true,"Build trees in parallel");
  protected final KeyArgument _modelKey = new KeyArgument(HTTP_MODEL_KEY,Key.make("model"),"Model key");
  protected final BooleanArgument _oobee = new BooleanArgument(HTTP_OOBEE,"Out of bag errors");
  protected final BooleanArgument _stratify = new BooleanArgument(HTTP_STRATIFY,"Stratify");




  @Override public void serve(JsonObject response) {
  }

}
/*

// @author cliffc
public class RandomForestPage extends H2OPage {
  public static final String DATA_KEY   = "Key";
  public static final String NUM_TREE   = "ntree";
  public static final String MAX_DEPTH  = "depth";
  public static final String SAMPLE     = "sample";
  public static final String BIN_LIMIT  = "binLimit";
  public static final String GINI       = "gini";
  public static final String RAND_SEED  = "seed";
  public static final String PARALLEL   = "parallel";
  public static final String MODEL_KEY  = "modelKey";
  public static final String CLASS_COL  = "class";
  public static final String IGNORE_COL = "ignore";
  public static final String OOBEE      = "OOBEE";
  public static final String FEATURES   = "features";

  public static final int MAX_CLASSES = 4096;

  @Override
  public String[] requiredArguments() {
    return new String[] { DATA_KEY };
  }

  public static String[] determineColumnClassNames(ValueArray ary, int classColIdx, int maxClasses) throws Page.PageError {
    int arity = ary.col_enum_domain_size(classColIdx);
    if (arity == 0) {
      int min = (int) ary.col_min(classColIdx);
      if (ary.col_min(classColIdx) != min)
        throw new Page.PageError("Only integer or enum columns can be classes!");
      int max = (int) ary.col_max(classColIdx);
      if (ary.col_max(classColIdx) != max)
        throw new Page.PageError("Only integer or enum columns can be classes!");
      if (max - min > maxClasses) // arbitrary number
        throw new Page.PageError("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
      String[] result = new String[max-min+1];
      for (int i = 0; i <= max - min; ++i)
        result[i] = String.valueOf(min+i);
      return result;
    } else {
      return  ary.col_enum_domain(classColIdx);
    }
  }


  public static double[] determineClassWeights(String source, ValueArray ary, int classColIdx, int maxClasses) throws Page.PageError {
    assert classColIdx>=0 && classColIdx < ary.num_cols();
    // determine the arity of the column
    HashMap<String,Integer> classNames = new HashMap();
    String[] names = determineColumnClassNames(ary,classColIdx,maxClasses);
    for (int i = 0; i < names.length; ++i)
      classNames.put(names[i],i);
    if (source.isEmpty())
      return null;
    double[] result = new double[names.length];
    for (int i = 0; i < result.length; ++i)
      result[i] = 1;
    // now parse the given string and update the weights
    int start = 0;
    byte[] bsource = source.getBytes();
    while (start < bsource.length) {
      while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
      String className;
      double classWeight;
      int end = 0;
      if (bsource[start] == ',') {
        ++start;
        end = source.indexOf(',',start);
        className = source.substring(start,end);
        ++end;

      } else {
        end = source.indexOf('=',start);
        className = source.substring(start,end);
      }
      start = end;
      while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
      if (bsource[start]!='=')
        throw new Page.PageError("Expected = after the class name.");
      ++start;
      end = source.indexOf(',',start);
      if (end == -1) {
        classWeight = Double.parseDouble(source.substring(start));
        start = bsource.length;
      } else {
        classWeight = Double.parseDouble(source.substring(start,end));
        start = end + 1;
      }
      if (!classNames.containsKey(className))
        throw new Page.PageError("Class "+className+" not found!");
      result[classNames.get(className)] = classWeight;
    }
    return result;
  }

  @Override
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws Page.PageError {
    ValueArray ary = ServletUtil.check_array(p, DATA_KEY);
    int ntree = getAsNumber(p,NUM_TREE, 50);
    if( ntree <= 0 )
      throw new H2OPage.InvalidInputException("Number of trees "+ntree+" must be positive.");
    int depth = getAsNumber(p,MAX_DEPTH, Integer.MAX_VALUE);
    int binLimit = getAsNumber(p,BIN_LIMIT, 1024);
    int smp = getAsNumber(p,SAMPLE, 67);
    if( smp <= 0 || smp > 100 )
      throw new H2OPage.InvalidInputException("Sampling percent of "+smp+" has to be between 0 and 100");
    float sample = smp==0 ? 1.00f : (smp/100.0f);
    int gini = getAsNumber(p, GINI, Tree.StatType.GINI.ordinal());
    long seed = getAsNumber(p, RAND_SEED, 181247619891L);
    int par = getAsNumber(p, PARALLEL, 1);
    if( !(par == 0 || par == 1) )
      throw new H2OPage.InvalidInputException("Parallel tree building "+par+" must be either 0 or 1");
    boolean parallel =  par== 1;
    Tree.StatType statType = Tree.StatType.values()[gini];

    // Optionally, save the model
    Key modelKey = null;
    String skey = p.getProperty(MODEL_KEY, "model");
    if( skey.isEmpty() ) skey = "model";
    try {
      modelKey = Key.make(skey);
    } catch( IllegalArgumentException e ) {
      throw new Page.PageError("Not a valid key: "+ skey);
    }

    int features = getAsNumber(p,FEATURES,-1);
    if ((features!=-1) && ((features<=0) || (features>=ary.num_cols() - 1)))
      throw new Page.PageError("Number of features can only be between 1 and num_cols - 1");

    // Pick the column to classify
    int classcol = ary.num_cols()-1; // Default to the last column
    String clz = p.getProperty(CLASS_COL);
    if( clz != null ) {
      int[] clarr = parseVariableExpression(ary.col_names(), clz);
      if( clarr.length != 1 )
        throw new H2OPage.InvalidInputException("Class has to refer to exactly one column!");
      classcol = clarr[0];
      if( classcol < 0 || classcol >= ary.num_cols() )
        throw new H2OPage.InvalidInputException("Class out of range");
    }
    double[] classWt = determineClassWeights(p.getProperty("classWt",""), ary, classcol, MAX_CLASSES);

    // Pick columns to ignore
    String igz = p.getProperty(IGNORE_COL);
    if( igz!=null ) System.out.println("[RF] ignoring: " + igz);
    System.out.println("[RF] class column: " + classcol);
    int[] ignores =  igz == null ? new int[0] : parseVariableExpression(ary.col_names(), igz);

    if( ignores.length + 1 >= ary.num_cols() )
      throw new H2OPage.InvalidInputException("Cannot ignore every column");

    // Remove any prior model; about to overwrite it
    UKV.remove(modelKey);
    for( int i=0; i<=ntree; i++ ) { // Also, all related Confusions
      UKV.remove(Confusion.keyFor(modelKey,i,ary._key,classcol,true ));
      UKV.remove(Confusion.keyFor(modelKey,i,ary._key,classcol,false));
    }

    // Start the distributed Random Forest
    JsonObject res = new JsonObject();
    res.addProperty("h2o", H2O.SELF.toString());
    try {
      DRF drf = hex.rf.DRF.web_main(ary,ntree,depth, sample, (short)binLimit, statType,seed, classcol,ignores,modelKey,parallel,classWt,features);
      // Output a model with zero trees (so far).
      final int classes = (short)((ary.col_max(classcol) - ary.col_min(classcol))+1);
      Model model = new Model(modelKey,drf._treeskey,ary.num_cols(),classes,sample,ary._key,ignores);
      // Save it to the cloud
      UKV.put(modelKey,model);
      // Pass along all to the viewer
      res.addProperty("dataKey", ary._key.toString());
      res.addProperty("modelKey", modelKey.toString());
      res.addProperty(NUM_TREE, ntree);
      res.addProperty("class", classcol);
      res.addProperty("classWt", p.getProperty("classWt",""));
      res.addProperty("OOBEE",getBoolean(p,OOBEE));
    } catch(DRF.IllegalDataException e) {
      res.addProperty("error", H2OPage.error("Incorrect input data: " + e.getMessage()));
    }
    return res;
  }

  @Override protected String serveImpl(Server s, Properties p, String sessionID) throws Page.PageError {
    JsonObject json = serverJson(s, p, sessionID);
    if(!json.has("error")){
      RString response = new RString(html);
      response.replace(json);
      response.replace("message","<b>Please wait!</b> It may take some time to launch the computation of the trees. This page will be automatically updated once some information is available.");
      return response.toString();
    }
    return H2OPage.error(json.get("error").toString());
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=/Wait?dataKey=%$dataKey&modelKey=%$modelKey&ntree=%ntree&class=%class&classWt=%classWt&OOBEE=%OOBEE&_WAIT_TARGET=RFView&_WAIT_MESSAGE=%$message\">\n";
}
*/