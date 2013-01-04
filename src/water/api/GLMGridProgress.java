package water.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import hex.GLMSolver.GLMModel;
import water.*;
import water.parser.ParseStatus;
import water.web.RString;

public class GLMGridProgress extends Request {
  protected final H2OExistingKey _taskey = new H2OExistingKey(DEST_KEY);

  public static Response redirect(JsonObject resp, Key taskey) {
    JsonObject redir = new JsonObject();
    redir.addProperty(DEST_KEY, taskey.toString());
    return Response.redirect(resp, GLMGridProgress.class, redir);
  }

  @Override protected Response serve() {
    Value v = _taskey.value();
    GLMGridStatus status = new GLMGridStatus().read(new AutoBuffer(v.get(),0));

    JsonObject response = new JsonObject();
    response.addProperty(Constants.DEST_KEY, v._key.toString());
    Response r = status._working
      ? Response.poll(response,status.progress())
      : Response.done(response);
    r.setBuilder(Constants.DEST_KEY, new GridBuilder(status));
    return r;
  }

  private class GridBuilder extends ElementBuilder {
    final GLMGridStatus _status;
    GridBuilder( GLMGridStatus s ) { _status = s; }
    @Override
    public String build(Response response, JsonElement json, String contextName) {
      StringBuilder sb = new StringBuilder();
      int step = _status._progress;

      // Mention something at the top about which model I am currently working on
      RString R = new RString( "<div class='alert alert-%succ'>GLMGrid search on <a href='/Inspect?Key=%key'>%key</a>, %prog</div>");
      R.replace("succ",_status._working ? "warning" : "success");
      R.replace("key" ,_status._datakey);
      R.replace("prog",_status._working ? "working on "+_status.model_name(step) : "stopped with "+step+" models built");
      sb.append(R);

      // Display all completed models
      int i=0;
      for( int l1=0; l1<_status._lambda1s.length; l1++ )
        for( int t=0; t<_status._threshes.length; t++ ) {
          GLMModel m = _status._ms[l1][t];
          if( m == null ) break;
          String mname = _status.model_name(i++);
          sb.append("<h4>").append(mname).append("</h4>");
          JsonObject tmp = new JsonObject();
          tmp.add("GLMModel",m.toJson());
          sb.append(new GLM.GLMBuilder(m,null).build(response,tmp,"yoink"));
        }

      return sb.toString();
    }
  }
}
