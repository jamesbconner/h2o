
package water.api;

import com.google.gson.JsonObject;
import hex.rf.Model;
import water.Key;
import water.UKV;

/**
 *
 * @author peta
 */
public class RFView extends Request {

  protected final H2OHexKey _dataKey = new H2OHexKey(JSON_DATA_KEY);
  protected final ModelKey _modelKey = new ModelKey(JSON_MODEL_KEY);
  protected final H2OHexKeyCol _classCol = new H2OHexKeyCol(_dataKey,JSON_CLASS,0);
  protected final Int _numTrees = new Int(JSON_NUM_TREES,50,0,Integer.MAX_VALUE);
  protected final H2OCategoryWeights _weights = new H2OCategoryWeights(_dataKey, _classCol, JSON_WEIGHTS, 1);
  protected final Bool _oobee = new Bool(JSON_OOBEE,false,"Out of bag errors");
  protected final IgnoreHexCols _ignore = new IgnoreHexCols(_dataKey, _classCol, JSON_IGNORE);
  protected final Bool _noCM = new Bool(JSON_NO_CM, false,"Do not produce confusion matrix");

  @Override protected Response serve() {
    Model model = _modelKey.value();
    int[] ignores = _ignore.specified() ? _ignore.value() : model._ignoredColumns;
    double[] weights = _weights.value();


    

    JsonObject response = new JsonObject();
    response.addProperty(JSON_DATA_KEY, _dataKey.originalValue());
    response.addProperty(JSON_MODEL_KEY, _modelKey.originalValue());
    response.addProperty(JSON_CLASS, _classCol.value());
    response.addProperty(JSON_NUM_TREES, model.size());



    return Response.done(new JsonObject());
  }


  // ---------------------------------------------------------------------------
  // ModelKey
  // ---------------------------------------------------------------------------

  public class ModelKey extends TypeaheadInputText<Model> {

    public ModelKey(String name) {
      super(name, true, "WWWModelKeys", JSON_KEYS);
    }

    @Override protected Model parse(String input) throws IllegalArgumentException {
      try {
        return UKV.get(Key.make(input), new Model());
      } catch (Exception e) {
        throw new IllegalArgumentException("Key "+input+" is not found or is not a model key");
      }
    }

    @Override protected Model defaultValue() {
      return null;
    }

    @Override protected String queryDescription() {
      return "key of the RF model";
    }

  }

}
