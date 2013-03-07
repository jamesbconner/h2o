package water.api;

import hex.rf.RFModel;
import water.Key;
import water.util.RString;

import com.google.gson.JsonObject;

/**
 * Page for random forest scoring.
 *
 * Redirect directly to RF view.
 */
public class RFScore extends Request {

  protected final H2OHexKey          _dataKey  = new H2OHexKey(DATA_KEY);
  protected final RFModelKey         _modelKey = new RFModelKey(MODEL_KEY);
  protected final HexKeyClassCol     _classCol = new HexKeyClassCol(CLASS, _dataKey);
  protected final Int                _numTrees = new NTree(NUM_TREES, _modelKey);
  protected final H2OCategoryWeights _weights  = new H2OCategoryWeights(WEIGHTS, _modelKey, _dataKey, _classCol, 1);
  protected final Bool               _noCM     = new Bool(NO_CM, false,"Do not produce confusion matrix");
  protected final Bool               _clearCM  = new Bool(JSON_CLEAR_CM, false, "Clear cache of model confusion matrices");

  public static final String JSON_CONFUSION_KEY = "confusion_key";
  public static final String JSON_CLEAR_CM      = "clear_confusion_matrix";

  RFScore() {
    _numTrees._readOnly = true;
  }

  public static String link(Key k, String content) {
    return link(k, DATA_KEY, content);
  }

  /** Generates a link to this page - param is {DATA_KEY, MODEL_KEY} with respect to specified key. */
  public static String link(Key k, String param, String content) {
    RString rs = new RString("<a href='RFScore.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", param);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    RFModel model = _modelKey.value();
    JsonObject response = new JsonObject();
    response.addProperty(DATA_KEY, _dataKey.originalValue());
    response.addProperty(MODEL_KEY, _modelKey.originalValue());
    response.addProperty(CLASS, _classCol.value());
    response.addProperty(NUM_TREES, model._totalTrees);
    if (_weights.specified())
      response.addProperty(WEIGHTS, _weights.originalValue());

    return RFView.redirect(response);
  }
}
