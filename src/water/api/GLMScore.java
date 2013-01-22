package water.api;

import hex.GLMSolver.GLMException;
import hex.GLMSolver.GLMModel;
import hex.GLMSolver.GLMValidation;
import water.Key;
import water.ValueArray;
import water.api.GLM.GLMBuilder;
import water.web.RString;

import com.google.gson.JsonObject;

public class GLMScore extends Request {
    public static final String JSON_MODEL_KEY = "modelKey";

    public static final String JSON_ROWS = "rows";
    public static final String JSON_TIME = "time";
    public static final String JSON_COEFFICIENTS = "coefficients";

  //  protected final H2OKey _dataKey = new H2OHexKey(KEY);
    protected final H2OGLMModelKey _modelKey = new H2OGLMModelKey(JSON_MODEL_KEY);
    protected final H2OHexKey _dataKey = new H2OHexKey(KEY);


    public static String link(Key k, String content) {
      RString rs = new RString("<a href='GLMScore.query?%key_param=%$key'>%content</a>");
      rs.replace("key_param", JSON_MODEL_KEY);
      rs.replace("key", k.toString());
      rs.replace("content", content);
      return rs.toString();
    }

    protected final RSeq _thresholds = new RSeq(Constants.DTHRESHOLDS, false, new NumberSequence("0:1:0.01", false, 0.01),false);

    public GLMScore() {
    }

    static class GLMValidationBuilder extends ObjectBuilder {
      final GLMValidation _val;
      GLMValidationBuilder( GLMValidation v) { _val=v; }
      public String build(Response response, JsonObject json, String contextName) {
        StringBuilder sb = new StringBuilder();;
        GLMBuilder.validationHTML(_val,sb);
        return sb.toString();
      }
    }

    @Override protected Response serve() {
      try {
        JsonObject res = new JsonObject();
        ValueArray ary = _dataKey.value();
        GLMModel m = _modelKey.value();
        GLMValidation v = m.validateOn(ary, null, _thresholds.value().arr);
        m.store();
        // Display HTML setup
        Response r = Response.done(res);
        RString rs = new RString("<div class='alert'>Validation of model <a href='/Inspect.html?"+KEY+"=%modelKey'>%modelKey</a> on dataset <a href='/Inspect.html?"+KEY+"=%dataKey'>%dataKey</a></div>");
        rs.replace("modelKey", m.key());
        rs.replace("dataKey",_dataKey.originalValue());
        r.addHeader(rs.toString());
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

