
package water.api;

import com.google.gson.JsonObject;
import hex.rf.Confusion;
import hex.rf.DRF;
import hex.rf.Tree;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.security.Krb5AndCertsSslSocketConnector;
import water.Key;
import water.UKV;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class RF extends Request {

  protected final H2OHexKey _dataKey = new H2OHexKey(JSON_DATA_KEY);
  protected final H2OHexKeyCol _classCol = new H2OHexKeyCol(_dataKey,JSON_CLASS,0);
  protected final Int _numTrees = new Int(JSON_NUM_TREES,50,0,Integer.MAX_VALUE);
  protected final Bool _gini = new Bool(JSON_GINI,true,"use gini statistic (otherwise entropy is used)");
  protected final H2OCategoryWeights _weights = new H2OCategoryWeights(_dataKey, _classCol, JSON_WEIGHTS, 1);
  protected final Bool _stratify = new Bool(JSON_STRATIFY,false,"Use Stratified sampling");
  protected final H2OCategoryStrata _strata = new H2OCategoryStrata(_dataKey, _classCol, JSON_STRATA, 1);
  protected final H2OKey _modelKey = new H2OKey(JSON_MODEL_KEY,Key.make("model"));
  protected final Bool _oobee = new Bool(JSON_OOBEE,false,"Out of bag errors");
  protected final Int _features = new Int(JSON_FEATURES, null, 1, Integer.MAX_VALUE);
  protected final IgnoreHexCols _ignore = new IgnoreHexCols(_dataKey, _classCol, JSON_IGNORE);
  protected final Int _sample = new Int(JSON_SAMPLE, 67, 0, 100);
  protected final Int _binLimit = new Int(JSON_BIN_LIMIT,1024, 0,65535);
  protected final Int _depth = new Int(JSON_DEPTH,0,0,Integer.MAX_VALUE);
  protected final Int _seed = new Int(JSON_SEED,0);
  protected final Bool _parallel = new Bool(JSON_PARALLEL,true,"Build trees in parallel");


  public RF() {
    _stratify.setRefreshOnChange();
  }

  @Override protected void queryArgumentValueSet(Argument arg, Properties inputArgs) {
    if (arg == _stratify) {
      if (_stratify.value()) {
        _oobee.disable("OOBEE is only meaningful if stratify is not specified.", inputArgs);
      } else {
        _strata.disable("Strata is only meaningful if stratify is on.", inputArgs);
      }
    }
  }

  /** Fires the random forest computation.
   */
  @Override public Response serve() {
    JsonObject response = new JsonObject();
    ValueArray ary = _dataKey.value();
    int classCol = _classCol.value();
    Key dataKey = ary._key;
    Key modelKey = _modelKey.value();
    int ntree = _numTrees.value();
    int features = _features.value() == null ? -1 : _features.value();
    float sample = _sample.value() / 100.0f;
    Tree.StatType statType = _gini.value() ? Tree.StatType.GINI : Tree.StatType.ENTROPY;
    UKV.remove(modelKey);
    for (int i = 0; i < ntree; ++i) {
      UKV.remove(Confusion.keyFor(modelKey,i,dataKey,classCol,true));
      UKV.remove(Confusion.keyFor(modelKey,i,dataKey,classCol,false));
    }
    try {
      hex.rf.DRF.web_main(
              ary,
              ntree,
              _depth.value(),
              sample,
              (short)(int)_binLimit.value(),
              statType,
              _seed.value(),
              classCol,
              _ignore.value(),
              modelKey,
              _parallel.value(),
              _weights.value(),
              features,
              _stratify.value(),
              _strata.convertToMap()
              );
      response.addProperty(JSON_DATA_KEY, dataKey.toString());
      response.addProperty(JSON_MODEL_KEY, modelKey.toString());
      response.addProperty(JSON_NUM_TREES, ntree);
      response.addProperty(JSON_CLASS, classCol);
      if (_weights.specified()) {
        response.addProperty(JSON_WEIGHTS, _weights.originalValue());
      }
      response.addProperty(JSON_OOBEE, _oobee.value());
    } catch (DRF.IllegalDataException e) {
      return Response.error("Incorrect input data: "+e.getMessage());
    }

    return Response.done(response);
  }
}
