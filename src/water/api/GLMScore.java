package water.api;

import hex.GLMSolver.GLMException;
import hex.GLMSolver.GLMModel;
import hex.GLMSolver.GLMValidation;
import water.Key;
import water.ValueArray;
import water.api.GLM.GLMBuilder;
import water.web.RString;
import com.google.gson.JsonObject;

/**
 * Simple web page to trigger glm validation on another dataset.
 * The dataset must contain the same columns (NOTE:identified by names) or error is returned.
 *
 * @author tomasnykodym
 *
 */
public class GLMScore extends Request {
    public static final String JSON_MODEL_KEY = "modelKey";
    protected final H2OGLMModelKey _modelKey = new H2OGLMModelKey(JSON_MODEL_KEY,true);
    protected final H2OHexKey _dataKey = new H2OHexKey(KEY);

    public static String link(Key k, double threshold, String content) {
      RString rs = new RString("<a href='GLMScore.query?%key_param=%$key&thresholds=%threshold'>%content</a>");
      rs.replace("key_param", JSON_MODEL_KEY);
      rs.replace("key", k.toString());
      rs.replace("threshold", threshold);
      rs.replace("content", content);
      return rs.toString();
    }

    protected final RSeq _thresholds = new RSeq(Constants.DTHRESHOLDS, false, new NumberSequence("0:1:0.01", false, 0.01),false);

    public GLMScore() {}

    static class GLMValidationBuilder extends ObjectBuilder {
      final GLMValidation _val;
      GLMValidationBuilder( GLMValidation v) { _val=v; }
      public String build(Response response, JsonObject json, String contextName) {
        StringBuilder sb = new StringBuilder();
        GLMBuilder.validationHTML(_val,sb);
        return sb.toString();
      }
    }

    @Override protected Response serve() {
      try {
        JsonObject res = new JsonObject();
        ValueArray ary = _dataKey.value();
        GLMModel m = _modelKey.value();
        GLMValidation v = m.validateOn(ary, null, _thresholds.value()._arr);
        m.store();
        // Display HTML setup
        Response r = Response.done(res);
        r.setBuilder("", new GLMValidationBuilder(v));
        return r;
      }catch(GLMException e){
        return Response.error(e.getMessage());
      } catch (Throwable t) {
        t.printStackTrace();
        return Response.error(t.getMessage());
      }
    }
  }

