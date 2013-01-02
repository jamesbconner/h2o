package water.api;

import hex.*;
import hex.GLMSolver.GLMModel;
import hex.GLMSolver.GLMParams;
import hex.GLMSolver.Link;

import java.util.BitSet;

import water.*;
import water.api.RequestArguments.InputText;
import water.web.RString;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author cliffc
 */
public class GLMGrid extends Request {



  public static final String JSON_GLM_Y = "y";
  public static final String JSON_GLM_X = "x";
  public static final String JSON_GLM_NEG_X = "neg_x";
  public static final String JSON_GLM_FAMILY = "family";
  public static final String JSON_GLM_NORM = "norm";
  public static final String JSON_GLM_LAMBDA = "lambda_1";
  public static final String JSON_GLM_LAMBDA_2 = "lambda_2";
  public static final String JSON_GLM_RHO = "rho";
  public static final String JSON_GLM_ALPHA = "alpha";
  public static final String JSON_GLM_MAX_ITER = "max_iter";
  public static final String JSON_GLM_BETA_EPS = "beta_eps";
  public static final String JSON_GLM_WEIGHT = "weight";
  public static final String JSON_GLM_THRESHOLD = "threshold";
  public static final String JSON_GLM_XVAL = "xval";
  public static final String JSON_GLM_CASE = "case";
  public static final String JSON_GLM_LINK = "link";
  public static final String JSON_GLM_EXPAND_CAT = "expand_cat";

  public static final String JSON_ROWS = "rows";
  public static final String JSON_TIME = "time";
  public static final String JSON_COEFFICIENTS = "coefficients";

  protected final H2OHexKey _key = new H2OHexKey(KEY);
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(_key, JSON_GLM_Y);
  protected final IgnoreHexCols _x = new IgnoreHexCols2(_key, _y, JSON_GLM_X);



  protected final Str _lambda1 = new Str(JSON_GLM_LAMBDA, ""+LSMSolver.DEFAULT_LAMBDA); // TODO I do not know the bounds
  protected final Str _lambda2 = new Str(JSON_GLM_LAMBDA_2, ""+LSMSolver.DEFAULT_LAMBDA2);
  protected final Str _alpha = new Str(JSON_GLM_ALPHA, ""+LSMSolver.DEFAULT_ALPHA);
  protected final Str _rho = new Str(JSON_GLM_RHO, ""+LSMSolver.DEFAULT_RHO); // TODO I do not know the bounds

  protected final Int _maxIter = new Int(JSON_GLM_MAX_ITER, GLMSolver.DEFAULT_MAX_ITER, 1, 1000000);
  protected final Real _weight = new Real(JSON_GLM_WEIGHT,1.0);
  protected final Real _case = new Real(JSON_GLM_CASE, Double.NaN);
  protected final EnumArgument<Link> _link = new EnumArgument(JSON_GLM_LINK,Link.familyDefault);
  protected final Int _xval = new Int(JSON_GLM_XVAL, 10, 0, 1000000);

  protected final Bool _expandCat = new Bool(JSON_GLM_EXPAND_CAT,false,"Expand categories");
  protected final Real _betaEps = new Real(JSON_GLM_BETA_EPS,GLMSolver.DEFAULT_BETA_EPS);


  // The "task key" for this Grid search.  Used to track job progress, to shutdown
  // early, to collect best-so-far & grid results, etc.
  final GLMGridTask _task = new GLMGridTask();


  protected static double [] parsePRange(String str){
    str = str.trim().toLowerCase();
    if(str.startsWith("seq")){
      throw new Error("unimplemented");
    } if(str.contains(":")){
      String [] parts = str.split(":");
      if(parts.length != 3)throw new Error("unexpected sequence format \"" + str + "\"");
      double from = Double.parseDouble(parts[0]);
      double to = Double.parseDouble(parts[1]);
      double step = Double.parseDouble(parts[2]);
      if(to == from) return new double[]{from};
      if(to < from)throw new Error("");
      if(step == 0)throw new Error();
      int n = (int)((Math.log(to) - Math.log(from))/Math.log(step));
      double [] res = new double[n];
      for(int i = 0; i < n; ++i){
        res[i] = from;
        from *= step;
      }
      return res;
    } else if(str.contains(",")){
      String [] parts = str.split(",");
      double [] res = new double[parts.length];
      for(int i = 0; i < parts.length; ++i)
        res[i] = Double.parseDouble(parts[i]);
      return res;
    } else {
      return new double [] {Double.parseDouble(str)};
    }
  }
  @Override protected Response serve() {
    try {
      if( _task._progress == 0 && !_task._stop && !_task._working ) {
        _task._ary = _key.value();
        _task._y = _y.value();
        _task._xs = _x.value();
        _task._lambda1Vec = parsePRange(_lambda1.value());
        _task._lambda2Vec = parsePRange(_lambda2.value());
        _task._rhoVec = parsePRange(_rho.value());
        _task._alphaVec = parsePRange(_alpha.value());
        _task._progress_max = _task._lambda1Vec.length*_task._lambda2Vec.length*_task._rhoVec.length*_task._alphaVec.length;
        if(_task._ms == null || _task._ms.length < _task._progress_max)
          _task._ms = new GLMModel[_task._progress_max];
        _task.start();          // One-shot start the task
      }

      JsonObject res = _task.toJson();

      Response r = !_task._working
        ? Response.done(res)
        : Response.poll(res,_task._progress,_task._progress_max);
      r.setBuilder(""/*top-level do-it-all builder*/,new GridBuilder());
      return r;

    } catch (Throwable t) {
      t.printStackTrace();
      return Response.error(t.getMessage());
    }
  }

  private class GridBuilder extends ObjectBuilder {
    public String build(Response response, JsonObject json, String contextName) {
      StringBuilder sb = new StringBuilder();
      int step = _task._progress;

      // Mention something at the top about which model I am currently working on
      RString R = new RString( "<div class='alert alert-%succ'>GLMGrid search on <a href='/Inspect?Key=%key'>%key</a>, %prog</div>");
      R.replace("succ",_task._working ? "warning" : "success");
      R.replace("key" ,_task._ary._key);
      R.replace("prog",_task._working ? "working on "+_task.model_name(step) : "stopped");
      sb.append(R);

      // Display all completed models
      for( int i=0; i<step; i++ ) {
        String mname = _task.model_name(i);
        JsonElement je = json.get(mname);
        if( je != null ) {
          sb.append("<h4>").append(mname).append("</h4>");
          JsonObject tmp = new JsonObject();
          tmp.add("GLMModel",je);
          sb.append(new GLM.GLMBuilder(_task._ms[i],null).build(response,tmp,"yoink"));
        }
      }
      return sb.toString();
    }
  }

  // ------------
  // A generic "task" object with progress & shutdown & results.
  // Some F/J worker thread will be updating this task periodically,
  // as well as inspecting it for stop commands.
  private abstract static class Task extends DTask<Task> {
    // Request any F/J worker thread to stop working on this task
    public volatile boolean _stop;
    // Set when a top-level F/J worker thread starts up,
    // and cleared when it shuts down.
    public volatile boolean _working;

    // Tracking of task progress.  This should be bumped each time there is a
    // coherent atomic piece of progress made.
    public volatile int _progress;
    // Estimated limit of progress, till all is done.
    public int _progress_max;

    // Start working on this task.  Should be called single-threadedly
    public void start() {
      assert _working == false;
      _working = true;
      H2O.FJP_NORM.submit(this);
    }

    // Stop working on this task, at your next polling point.  Can be called
    // repeatedly from any time.
    public void stop() {
      _stop = true;
    }

    // Work on this task
    public void compute() {
      assert _working == true;
      while( !_stop && _progress < _progress_max )
        do_task(_progress++);
      _working = false;
      tryComplete();            // This task is done
    }

    // Not intended for remote or distributed execution; task control runs on
    // one node.
    public Task invoke( H2ONode sender ) { throw H2O.unimpl(); }

    // Do poll-able step #.
    // This is expected to be a *blocking* F/J step
    abstract void do_task( int step );

    // Convert to a JSON object
    JsonObject toJson() {
      JsonObject j = new JsonObject();
      j.addProperty("stop",_stop);
      j.addProperty("working",_working);
      j.addProperty("progress",_progress);
      j.addProperty("progress_max",_progress_max);
      return j;
    }
  }

  // A grid-search task
  private static class GLMGridTask extends Task {
    ValueArray _ary;            // Array to work on
    int _y;                     // Y column; class
    int _xs[];                  // Array of columns to use
    double [] _lambda1Vec;
    double [] _lambda2Vec;
    double [] _rhoVec;
    double [] _alphaVec;

    // Compute 11 threshold GLM models
    GLMModel _ms[];

    // Do a single step (blocking).
    // In this case, run 1 GLM model.
    void do_task( int step ) {

      GLMParams glmp = new GLMParams();
      glmp._f = GLMSolver.Family.binomial;
      glmp._l = glmp._f.defaultLink;
      glmp._expandCat = true;
      glmp._maxIter = 20;
      glmp._betaEps = GLMSolver.DEFAULT_BETA_EPS;
      glmp._familyArgs = glmp._f.defaultArgs; // no case/weight.  default 0.5 thresh
      //glmp._familyArgs[GLMSolver.FAMILY_ARGS_DECISION_THRESHOLD] = thresh(step);

      // Always use elastic-net
      LSMSolver lsms = LSMSolver.makeElasticNetSolver(lambda1(step), lambda2(step), rho(step), alpha(step));
      GLMSolver glm = new GLMSolver(lsms, glmp);
      GLMModel m = _ms[step] = glm.xvalidate(_ary, createColumns(),10)[0]; // fixme, it should contain link to crossvalidatoin results and aggreaget info
    }

    // Threshold from total progress bar
    final double thresh( int step ) {
      return (double)step/(_ms.length-1);
    }

    // lambda1 from total progress bar
    final double lambda1(int step){
      int idx = step/(_lambda2Vec.length*_alphaVec.length*_rhoVec.length);
      return _lambda1Vec[idx];
    }

    // lambda2 from total progress bar
    final double lambda2(int step){
      int idx = step/(_alphaVec.length*_rhoVec.length);
      return _lambda2Vec[idx % _lambda2Vec.length];
    }

 // alpha from total progress bar
    final double alpha(int step){
      int idx = step/(_rhoVec.length);
      return _alphaVec[idx % _alphaVec.length];
    }

 // rho from total progress bar
    final double rho(int step){
      return _rhoVec[step % _rhoVec.length];
    }

    String model_name( int step ) {
      return "GLMModel["+step + "]";
    }


    private int[] createColumns() {
      BitSet cols = new BitSet();
      for( int i : _xs ) cols.set(i);
      int[] res = new int[cols.cardinality()+1];
      int x=0;
      for( int i = cols.nextSetBit(0); i >= 0; i = cols.nextSetBit(i+1))
        res[x++] = i;
      res[x] = _y;
      return res;
    }


    // Convert all models to Json (expensive!)
    JsonObject toJson() {
      JsonObject j = super.toJson();
      for( int i=0; i<_ms.length; i++ )
        if( _ms[i] != null )
          j.add(model_name(i),_ms[i].toJson());
      return j;
    }
  }
}
