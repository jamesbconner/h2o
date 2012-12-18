
package water.api;

import com.google.gson.JsonObject;
import hex.rf.Tree;
import water.Key;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class RF extends Request {

  protected final H2OHexKey _dataKey = new H2OHexKey(JSON_DATA_KEY);
  protected final H2OHexKeyCol _classCol = new H2OHexKeyCol(_dataKey,JSON_CLASS,0);
  protected final Int _numTrees = new Int(JSON_NUM_TREES,50,0,Integer.MAX_VALUE);
  protected final Int _depth = new Int(JSON_DEPTH,0,0,Integer.MAX_VALUE);
  protected final Int _sample = new Int(JSON_SAMPLE, 67, 0, 100);
  protected final Int _binLimit = new Int(JSON_BIN_LIMIT,1024, 0,65535);
  protected final Bool _gini = new Bool(JSON_GINI,true,"use gini statistic (otherwise entropy is used)");
  protected final Int _seed = new Int(JSON_SEED,0);
  protected final Bool _parallel = new Bool(JSON_PARALLEL,true,"Build trees in parallel");
  protected final H2OKey _modelKey = new H2OKey(JSON_MODEL_KEY,Key.make("model"));
  protected final Bool _oobee = new Bool(JSON_OOBEE,false,"Out of bag errors");
  protected final Bool _stratify = new Bool(JSON_STRATIFY,false,"Use Stratified sampling");
//  protected final H2OCategoryDoubles _weights = new H2OCategoryDoubles(_dataKey, _classCol, JSON_WEIGHTS,1,"Category weights",0,Double.POSITIVE_INFINITY);
//  protected final H2OKeyCols _ignore = new H2OKeyCols(_dataKey,JSON_IGNORE,new int[0],"Ignore columns");
  protected final Int _features = new Int(JSON_FEATURES, 0, 0, Integer.MAX_VALUE);
  protected final IgnoreHexCols _ignore = new IgnoreHexCols(_dataKey, _classCol, JSON_IGNORE);
  protected final H2OCategoryWeights _weights = new H2OCategoryWeights(_dataKey, _classCol, JSON_WEIGHTS, 1);

  /** Fires the random forest computation.
   *
   * @param response
   */
  @Override public Response serve() {
    JsonObject response = new JsonObject();
    ValueArray ary = _dataKey.value();
    Key modelKey = _modelKey.value();
    // TODO this is ugly and should be changed completely
    Tree.StatType statType = Tree.StatType.values()[_gini.value() ? 1 : 0];
    int classCol = _classCol.value();
    float sample = _sample.value() / 100f;
//    double[] weights = _weights.value();
    try {
   /*   DRF drf = hex.rf.DRF.web_main(
              ary,
              _numTrees.value(),
              _depth.value(),
              sample,
              (short)(int)_binLimit.value(),
              statType,
              _seed.value(),
              classCol,
              _ignore.value(),
              modelKey,
              _parallel.value(),
              weights,
              _features.value()
              ); */
      // Output a model with zero trees (so far).
//      final int classes = (short)((ary.col_max(classCol) - ary.col_min(classCol))+1);
      // Output a model with zero trees (so far).
     // Model model = new Model(modelKey,drf._treeskey,ary.num_cols(),classes,sample,ary._key,_ignore.value());
      // Save it to the cloud
     // UKV.put(modelKey,model);
      // Pass along all to the viewer */
      response.addProperty(JSON_DATA_KEY, ary._key.toString());
      response.addProperty(JSON_MODEL_KEY, modelKey.toString());
      response.addProperty(JSON_NUM_TREES, _numTrees.value());
      response.addProperty(JSON_CLASS, classCol);
//      response.addProperty(JSON_CLASS_WEIGHT, p.getProperty("classWt",""));
//      response.addProperty(JSON_OOBEE,getBoolean(p,OOBEE));
      return Response.done(response);
    } catch (Exception e) {
      return Response.error("Cannot start the RF computation. The following error was reported: "+e.getMessage());
    }

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