package water.web;

import com.google.gson.JsonObject;
import hex.rf.*;
import hex.rf.Tree.StatType;
import java.util.HashMap;
import java.util.Properties;
import water.*;

// @author cliffc
public class RandomForestPage extends H2OPage {
  public static final String DATA_KEY   = "Key";
  public static final String NUM_TREE   = "ntree";
  public static final String MAX_DEPTH  = "depth";
  public static final String SAMPLE     = "sample";
  public static final String BIN_LIMIT  = "binlimit";
  public static final String GINI       = "gini";
  public static final String RAND_SEED  = "seed";
  public static final String PARALLEL   = "parallel";
  public static final String MODEL_KEY  = "modelKey";
  public static final String CLASS_COL  = "class";
  public static final String IGNORE_COL = "ignore";

  public static final int MAX_CLASSES = 4096;

  @Override
  public String[] requiredArguments() {
    return new String[] { DATA_KEY };
  }

  public static String[] determineColumnClassNames(ValueArray ary, int classColIdx, int maxClasses) throws PageError {
    int arity = ary.col_enum_domain_size(classColIdx);
    if (arity == 65535) {
      int min = (int) ary.col_min(classColIdx);
      if (ary.col_min(classColIdx) != min)
        throw new PageError("Only integer or enum columns can be classes!");
      int max = (int) ary.col_max(classColIdx);
      if (ary.col_max(classColIdx) != max)
        throw new PageError("Only integer or enum columns can be classes!");
      if (max - min > maxClasses) // arbitrary number
        throw new PageError("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
      String[] result = new String[max-min+1];
      for (int i = 0; i <= max - min; ++i)
        result[i] = String.valueOf(min+i);
      return result;
    } else {
      return  ary.col_enum_domain(classColIdx);
    }
  }


  public static double[] determineClassWeights(String source, ValueArray ary, int classColIdx, int maxClasses) throws PageError {
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
        throw new PageError("Expected = after the class name.");
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
        throw new PageError("Class "+className+" not found!");
      result[classNames.get(className)] = classWeight;
    }
    return result;
  }

  @Override
  public JsonObject serverJson(Server s, Properties p, String sessionID) throws PageError {
    ValueArray ary = ServletUtil.check_array(p, DATA_KEY);
    int ntree = getAsNumber(p,NUM_TREE, 5);
    if( ntree <= 0 )
      throw new InvalidInputException("Number of trees "+ntree+" must be positive.");
    int depth = getAsNumber(p,MAX_DEPTH, Integer.MAX_VALUE);
    int binLimit = getAsNumber(p,BIN_LIMIT, 1024);
    int smp = getAsNumber(p,SAMPLE, 67);
    if( smp <= 0 || smp > 100 )
      throw new InvalidInputException("Sampling percent of "+smp+" has to be between 0 and 100");
    float sample = smp==0 ? 1.00f : (smp/100.0f);
    int gini = getAsNumber(p, GINI, StatType.GINI.ordinal());
    int seed = getAsNumber(p, RAND_SEED, 42);
    int par = getAsNumber(p, PARALLEL, 1);
    if( !(par == 0 || par == 1) )
      throw new InvalidInputException("Parallel tree building "+par+" must be either 0 or 1");
    boolean parallel =  par== 1;
    StatType statType = StatType.values()[gini];

    // Optionally, save the model
    Key modelKey = null;
    String skey = p.getProperty(MODEL_KEY, "model");
    if( skey.isEmpty() ) skey = "model";
    try {
      modelKey = Key.make(skey);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }

    // Pick the column to classify
    int classcol = ary.num_cols()-1; // Default to the last column
    String clz = p.getProperty(CLASS_COL);
    if( clz != null ) {
      int[] clarr = parseVariableExpression(ary.col_names(), clz);
      if( clarr.length != 1 )
        throw new InvalidInputException("Class has to refer to exactly one column!");
      classcol = clarr[0];
      if( classcol < 0 || classcol >= ary.num_cols() )
        throw new InvalidInputException("Class out of range");
    }
    double[] classWt = determineClassWeights(p.getProperty("classWt",""), ary, classcol, MAX_CLASSES);

    // Pick columns to ignore
    String igz = p.getProperty(IGNORE_COL);
    System.out.println("[RF] ignoring: " + igz);
    System.out.println("[RF] class column: " + classcol);
    int[] ignores =  igz == null ? new int[0] : parseVariableExpression(ary.col_names(), igz);

    if( ignores.length + 1 >= ary.num_cols() )
      throw new InvalidInputException("Cannot ignore every column");

    // Remove any prior model; about to overwrite it
    UKV.remove(modelKey);
    for( int i=0; i<=ntree; i++ ) // Also, all related Confusions
      UKV.remove(Confusion.keyFor(modelKey,i,ary._key,classcol));

    // Start the distributed Random Forest
    JsonObject res = new JsonObject();
    res.addProperty("h2o",H2O.SELF.urlEncode());
    try {
      DRF drf = hex.rf.DRF.web_main(ary,ntree,depth, sample, (short)binLimit, statType,seed, classcol,ignores,modelKey,parallel,classWt);
      // Output a model with zero trees (so far).
      final int classes = (short)((ary.col_max(classcol) - ary.col_min(classcol))+1);
      Model model = new Model(modelKey,drf._treeskey,ary.num_cols(),classes);
      // Save it to the cloud
      UKV.put(modelKey,model);
      // Pass along all to the viewer
      res.addProperty("dataKey", ary._key.toString());
      res.addProperty("modelKey", modelKey.toString());
      res.addProperty(NUM_TREE, ntree);
      res.addProperty("class", classcol);
      res.addProperty("classWt", p.getProperty("classWt",""));
    } catch(DRF.IllegalDataException e) {
      res.addProperty("error", H2OPage.error("Incorrect input data: " + e.getMessage()));
    }
    return res;
  }

  @Override protected String serveImpl(Server s, Properties p, String sessionID) throws PageError {
    JsonObject json = serverJson(s, p, sessionID);
    if(!json.has("error")){
      RString response = new RString(html);
      response.replace(json);
      return response.toString();
    }
    return H2OPage.error(json.get("error").toString());
  }
  final static String html =
    "<meta http-equiv=\"REFRESH\" content=\"0;url=/RFView?dataKey=%$dataKey&modelKey=%$modelKey&ntree=%ntree&class=%class&classWt=%classWt\">\n";
}
