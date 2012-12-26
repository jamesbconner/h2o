package hex.rf;
import hex.rf.Tree.StatType;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import water.*;

/**
 * A RandomForest can be used for growing or validation. The former starts with a known target number of trees,
 * the latter is incrementally populated with trees as they are built.
 * Validation and error reporting is not supported when growing a forest.
 */
public class RandomForest {
  final Data _data;             // The data to train on.
  private int _features;        // features to check at each split
  boolean _stratify;
  int [] _strata;

  public RandomForest(DRF drf, Data data, int ntrees, int maxTreeDepth, double minErrorRate, StatType stat, boolean parallelTrees, int features, int[] ignoreColumns) {
    // Build N trees via the Random Forest algorithm.
    _data = data;
    _features = features;
    _stratify = drf._useStratifySampling;
    _strata = drf._strata;
    Timer t_alltrees = new Timer();
    Tree[] trees = new Tree[ntrees];
    Random rnd = new Random(data.seed());
    for (int i = 0; i < ntrees; ++i) {
      trees[i] = new Tree(_data,maxTreeDepth,minErrorRate,stat,features(),rnd.nextLong(), drf._modelKey,i,drf._ntrees, drf._sample, drf._numrows, ignoreColumns,_stratify, _strata);
      if (!parallelTrees) DRemoteTask.invokeAll(new Tree[]{trees[i]});
    }
    if(parallelTrees)DRemoteTask.invokeAll(trees);
    Utils.pln("All trees ("+ntrees+") done in "+ t_alltrees);
  }

  public int features() { return _features; }

  public static Map<Integer,Integer> parseStrata(String s){
    if(s.isEmpty())return null;
    String [] strs = s.split(",");
    Map<Integer,Integer> res = new HashMap<Integer, Integer>();
    for(String x:strs){
      String [] arr = x.split(":");
      res.put(Integer.parseInt(arr[0].trim()), Integer.parseInt(arr[1].trim()));
    }
    return res;
  }

}
