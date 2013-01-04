package water.api;

import com.google.gson.JsonObject;
import hex.*;
import hex.GLMSolver.GLMModel;
import hex.GLMSolver.GLMParams;
import hex.GLMSolver.Link;
import java.util.BitSet;
import water.*;


// A grid-search task.  This task is embedded in a Value and mapped to a Key,
// and only can be updated via Atomic ops on the mapped Value.  So basically,
// all these fields are "final" in this POJO and are modified by atomic update.
class GLMGridStatus extends DTask<GLMGridStatus> {
  // Self-key - actual data is stored in the K/V store.  This is just a
  // convenient POJO to read the bits.
  Key _taskey;                // Myself key

  // Request any F/J worker thread to stop working on this task
  boolean _stop;
  // Set when a top-level F/J worker thread starts up,
  // and cleared when it shuts down.
  boolean _working = true;
    
  Key _datakey;               // Datakey to work on
  transient ValueArray _ary;  // Expanded VA bits
  int _ccol;                  // Y column; class
  int _xs[];                  // Array of columns to use
  double [] _lambda1s;        // Grid search values
  double [] _lambda2s;        // Grid search values
  double [] _rhos;            // Grid search values
  double [] _alphas;          // Grid search values
  double [] _threshes;        // Grid search values

  // Progress: Count of GLM models computed so far.
  int _progress;
  int _max;

  // The computed GLM models: product of length of lamda1s,lambda2s,rhos,alphas
  GLMModel _ms[][];

  // Fraction complete
  float progress() { return (float)_progress/_ms.length; }

  public GLMGridStatus(Key taskey, ValueArray va, int ccol, int[] xs, double[]l1s, double[]l2s, double[]rs, double[]as, double[]ts) {
    _taskey = taskey;           // Capture the args
    _ary = va;                  // VA is large, and already in a Key so make it transient
    _datakey = va._key;         // ... and use the datakey instead when reloading
    _ccol = ccol;
    _xs = xs;
    _lambda1s = l1s;
    _lambda2s = l2s;
    _rhos     = rs;
    _alphas   = as;
    _threshes = ts;
    _max = l1s.length*ts.length;
    _ms = new GLMModel[l1s.length][ts.length];
  }
  // Void constructor for serialization
  public GLMGridStatus() {}

  // Work on this task.  Only *this* main thread updates '_working' &
  // '_progress' & '_ms' fields.  Only other web threads update the '_stop'
  // field; web threads also atomically-read the progress&ms fields.
  public void compute() {
    assert _working == true;
    OUTER: 
    for( int l1=0; l1<_lambda1s.length; l1++ )
      for( int t=0; t<_threshes.length; t++ ) {
        if( _stop ) break OUTER;

        final GLMModel m = do_task(l1,0,0,0,t); // Do a step; get a model

        // Now update this Status. 
        final int l1f = l1;
        final int tf = t;
        new TAtomic<GLMGridStatus>() {
          @Override public GLMGridStatus atomic(GLMGridStatus old) {
            old._ms[l1f][tf] = m; old._progress++; return old; }
          @Override public GLMGridStatus alloc() { return new GLMGridStatus(); }
        }.invoke(_taskey);
        // Fetch over the 'this' all new bits.  Mostly witness updates to
        // _progress and _stop fields.
        UKV.get(_taskey,this);
      }

    // Update _working to 'false' - we have stopped working
    new TAtomic<GLMGridStatus>() {
      @Override public GLMGridStatus atomic(GLMGridStatus old) {
        old._working = false; return old; }
      @Override public GLMGridStatus alloc() { return new GLMGridStatus(); }
    }.invoke(_taskey);

    tryComplete();            // This task is done
  }

  // ---
  // Do a single step (blocking).
  // In this case, run 1 GLM model.
  private GLMModel do_task( int l1, int l2, int rho, int alpha, int t ) {
    // Default GLM args.  Binomial, logit, expanded cats, max iter, etc
    GLMParams glmp = new GLMParams();
    glmp._f = GLMSolver.Family.binomial;
    glmp._l = glmp._f.defaultLink;
    glmp._expandCat = true;
    glmp._maxIter = 20;
    glmp._betaEps = GLMSolver.DEFAULT_BETA_EPS;
    glmp._familyArgs = glmp._f.defaultArgs; // no case/weight.  default 0.5 thresh

    // The 'step' is the folded iterator
    // Break the step into iterations over the various parameters.
    glmp._familyArgs[GLMSolver.FAMILY_ARGS_DECISION_THRESHOLD] = _threshes[t];

    // Always use elastic-net, but set the various parameters
    LSMSolver lsms = LSMSolver.makeElasticNetSolver(_lambda1s[l1], _lambda2s[l2], _rhos[rho], _alphas[alpha]);
    GLMSolver glm = new GLMSolver(lsms, glmp);
    //GLMModel m = glm.xvalidate(_ary, createColumns(),10)[0]; // fixme, it should contain link to crossvalidatoin results and aggreaget info
    GLMModel m = glm.computeGLM(_ary,createColumns(),null);
    return m;
  }
  // ---

  String model_name( int step ) {
    return "GLMModel["+step+"]";
  }

  private int[] createColumns() {
    BitSet cols = new BitSet();
    for( int i : _xs ) cols.set(i);
    int[] res = new int[cols.cardinality()+1];
    int x=0;
    for( int i = cols.nextSetBit(0); i >= 0; i = cols.nextSetBit(i+1))
      res[x++] = i;
    res[x] = _ccol;
    return res;
  }

  // Convert all models to Json (expensive!)
  public JsonObject toJson() {
    JsonObject j = new JsonObject();
    for( int l1=0; l1<_lambda1s.length; l1++ )
      for( int t=0; t<_threshes.length; t++ )
        if( _ms[l1][t] != null )
          j.add(model_name(_progress),_ms[l1][t].toJson());
    return j;
  }
  // Not intended for remote or distributed execution; task control runs on
  // one node.
  public GLMGridStatus invoke( H2ONode sender ) { throw H2O.unimpl(); }
}
