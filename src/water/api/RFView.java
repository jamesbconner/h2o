
package water.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import hex.rf.Confusion;
import hex.rf.Model;
import water.Key;
import water.UKV;

/**
 *
 * @author peta
 */
public class RFView extends Request {

  protected final H2OHexKey _dataKey = new H2OHexKey(DATA_KEY);
  protected final ModelKey _modelKey = new ModelKey(MODEL_KEY);
  protected final H2OHexKeyCol _classCol = new H2OHexKeyCol(_dataKey,CLASS,0);
  protected final Int _numTrees = new Int(NUM_TREES,50,0,Integer.MAX_VALUE);
  protected final H2OCategoryWeights _weights = new H2OCategoryWeights(_dataKey, _classCol, WEIGHTS, 1);
  protected final Bool _oobee = new Bool(OOBEE,false,"Out of bag errors");
  protected final IgnoreHexCols _ignore = new IgnoreHexCols(_dataKey, _classCol, IGNORE);
  protected final Bool _noCM = new Bool(NO_CM, false,"Do not produce confusion matrix");
  protected final Bool _clearCM = new Bool(JSON_CLEAR_CM, false, "Clear cache of model confusion matrices");

  public static final String JSON_BUILT_TREES = "built_trees";
  public static final String JSON_CONFUSION_KEY = "confusion_key";
  public static final String JSON_CLEAR_CM = "clear_cm";
  public static final String JSON_CM_FLAVOUR = "flavour";
  public static final String JSON_CM_HEADER = "header";
  public static final String JSON_CM_MATRIX = "matrix";
  public static final String JSON_CM = "confusion_matrix";
  public static final String JSON_CM_TREES = "used_trees";
  public static final String JSON_TREES = "trees";
  public static final String JSON_TREE_DEPTH = "depth";
  public static final String JSON_TREE_LEAVES = "leaves";


  private static final ConfusionMatrixBuilder CONFUSION_BUILDER = new ConfusionMatrixBuilder();

  @Override protected Response serve() {
    int tasks = 0;
    int finished = 0;
    Model model = _modelKey.value();
    int[] ignores = _ignore.specified() ? _ignore.value() : model._ignoredColumns;
    double[] weights = _weights.value();
    JsonObject response = new JsonObject();

    response.addProperty(DATA_KEY, _dataKey.originalValue());
    response.addProperty(MODEL_KEY, _modelKey.originalValue());
    response.addProperty(CLASS, _classCol.value());
    response.addProperty(NUM_TREES, model._totalTrees);
    response.addProperty(JSON_BUILT_TREES, model.size());

    tasks += model._totalTrees;
    finished += model.size();

    // CM return and possible computation is requested
    if (!_noCM.value()) {
      tasks += 1;
      // get the confusion matrix
      Confusion confusion = Confusion.make(model, _dataKey.value()._key, _classCol.value(), ignores, weights, _oobee.value());
      response.addProperty(JSON_CONFUSION_KEY, confusion.keyFor().toString());
      // if the matrix is valid, report it in the JSON
      if (confusion.isValid()) {
        finished += 1;
        JsonObject cm = new JsonObject();
        JsonArray cmHeader = new JsonArray();
        JsonArray matrix = new JsonArray();
        cm.addProperty(JSON_CM_FLAVOUR, _oobee.value() ? "OOB error estimate" : "full scoring");
        // create the header
        for (String s : vaCategoryNames(_dataKey.value()._cols[_classCol.value()],1024))
          cmHeader.add(new JsonPrimitive(s));
        cm.add(JSON_CM_HEADER,cmHeader);
        // add the matrix
        for (int crow = 0; crow < model._classes; ++crow) {
          JsonArray row = new JsonArray();
          for (int ccol = 0; ccol < model._classes; ++ccol)
            row.add(new JsonPrimitive(confusion._matrix[crow][ccol]));
          matrix.add(row);
        }
        cm.add(JSON_CM_MATRIX,matrix);
        cm.addProperty(JSON_CM_TREES,confusion._treesUsed);
        response.add(JSON_CM,cm);
      }
    }

    // Trees

    JsonObject trees = new JsonObject();
    trees.addProperty(JSON_TREE_DEPTH,model.depth());
    trees.addProperty(JSON_TREE_LEAVES, model.leaves());

    response.add(JSON_TREES,trees);

    JsonObject pollArgs = argumentsToJson();
    //pollArgs.addProperty(JSON_NO_CM,"1"); // not yet - CM runs in the same thread TODO
    Response r = (finished == tasks) ? Response.done(response) : Response.poll(response, finished, tasks, pollArgs);
    r.setBuilder(JSON_CM, CONFUSION_BUILDER);
    return r;
  }

  // ---------------------------------------------------------------------------
  // ConfusionMatrixBuilder
  // ---------------------------------------------------------------------------

  public static class ConfusionMatrixBuilder extends ObjectBuilder {
    @Override public String build(Response response, JsonObject cm, String contextName) {
      StringBuilder sb = new StringBuilder();
      if (cm.has(JSON_CM_MATRIX)) {
        sb.append("<h3>Confusion matrix - "+cm.get(JSON_CM_FLAVOUR).getAsString()+"</h3>");
        sb.append("<table class='table table-striped table-bordered table-condensed'>");
        sb.append("<tr><th>Actual \\ Predicted</th>");
        JsonArray header = (JsonArray) cm.get(JSON_CM_HEADER);
        for (JsonElement e: header)
          sb.append("<th>"+e.getAsString()+"</th>");
        sb.append("<th>Error</th></tr>");
        int classes = header.size();
        long[] totals = new long[classes];
        JsonArray matrix = (JsonArray) cm.get(JSON_CM_MATRIX);
        long sumTotal = 0;
        long sumError = 0;
        for (int crow = 0; crow < classes; ++crow) {
          JsonArray row = (JsonArray) matrix.get(crow);
          long total = 0;
          long error = 0;
          sb.append("<tr><th>"+header.get(crow).getAsString()+"</th>");
          for (int ccol = 0; ccol < classes; ++ccol) {
            long num = row.get(ccol).getAsLong();
            total += num;
            totals[ccol] += num;
            if (ccol == crow) {
              sb.append("<td style='background-color:LightGreen'>");
            } else {
              sb.append("<td>");
              error += num;
            }
            sb.append(num);
            sb.append("</td>");
          }
          sb.append("<td>");
          sb.append(String.format("%5.3f = %d / %d", (double)error/total, error, total));
          sb.append("</td></tr>");
          sumTotal += total;
          sumError += error;
        }
        sb.append("<tr><th>Totals</th>");
        for (int i = 0; i < totals.length; ++i)
          sb.append("<td>"+totals[i]+"</td>");
        sb.append("<td><b>");
        sb.append(String.format("%5.3f = %d / %d", (double)sumError/sumTotal, sumError, sumTotal));
        sb.append("</b></td></tr>");
        sb.append("</table>");
        sb.append("Trees used: "+cm.get(JSON_CM_TREES).getAsInt());
      } else {
        sb.append("<div class='alert alert-info'>");
        sb.append("Confusion matrix is being computed into the key:</br>");
        sb.append(cm.get(JSON_CONFUSION_KEY).getAsString());
        sb.append("</div>");
      }
      return sb.toString();
    }
  }


  // ---------------------------------------------------------------------------
  // ModelKey
  // ---------------------------------------------------------------------------

  public class ModelKey extends TypeaheadInputText<Model> {

    public ModelKey(String name) {
      super(name, true, "WWWModelKeys", KEYS);
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
