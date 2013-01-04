package water.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import hex.GLMSolver;
import hex.GLMSolver.*;
import hex.LSMSolver;
import java.text.DecimalFormat;
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
    GridBuilder( GLMGridStatus s ) {
      _status = s;
    }
    @Override
    public String build(Response response, JsonElement json, String contextName) {
      StringBuilder sb = new StringBuilder();
      int step = _status._progress;
      int nclasses = 2; //TODO

      // Mention something at the top about which model I am currently working on
      RString R = new RString( "<div class='alert alert-%succ'>GLMGrid search on <a href='/Inspect?Key=%key'>%key</a>, %prog</div>");
      R.replace("succ",_status._working ? "warning" : "success");
      R.replace("key" ,_status._datakey);
      R.replace("prog",_status._working ? "working on "+_status.model_name(step) : "stopped with "+step+" models built");
      sb.append(R);

      sb.append("<table class='table table-bordered table-condensed'>");
      sb.append("<tr><th>Model</th><th>&lambda;<sub>1</sub></th><th>&lambda;<sub>2</sub></th><th>&rho;</th><th>&alpha;</th><th>Best Threshold</th><th>AUC</th>");
      for(int c = 0; c < nclasses; ++c)
        sb.append("<th>Err(" + c + ")</th>");
      sb.append("</tr>");

      // Display all completed models
      int i=0;
      for(GLMXValidation m:_status.computedModels()) {
        String mname = _status.model_name(i++);
        LSMSolver lsm = m.lsmSolver();
        sb.append("<tr>");
        sb.append("<td>" + "Model[" + i++ + "]"  + "</td>");
        sb.append("<td>" + sci_dformat.format(lsm._lambda) + "</td>");
        sb.append("<td>" + sci_dformat.format(lsm._lambda2) + "</td>");
        sb.append("<td>" + sci_dformat.format(lsm._rho) + "</td>");
        sb.append("<td>" + dformat.format(lsm._alpha) + "</td>");
        sb.append("<td>" + dformat.format(m.bestThreshold()) + "</td>");
        sb.append("<td>" + dformat.format(m.AUC()) + "</td>");
        for(double e:m.classError())
          sb.append("<td>" + dformat.format(e) + "</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
      return sb.toString();
    }
  }
  private static final DecimalFormat dformat = new DecimalFormat("###.###");
  private static final DecimalFormat sci_dformat = new DecimalFormat("#.#E0");
}
