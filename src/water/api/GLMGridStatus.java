package water.api;

import hex.*;
import hex.GLMSolver.ErrMetric;
import hex.GLMSolver.GLMParams;
import hex.GLMSolver.GLMXValidation;

import java.util.*;

import water.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


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
  double _caseval;            // Selected CASE/factor value or NaN usually
  double [] _lambda1s;        // Grid search values
  double [] _lambda2s;        // Grid search values
  double [] _rhos;            // Grid search values
  double [] _alphas;          // Grid search values

  // Progress: Count of GLM models computed so far.
  int _progress;
  int _max;

  // The computed GLM models: product of length of lamda1s,lambda2s,rhos,alphas
  GLMXValidation _ms[];

  // Fraction complete
  float progress() { return (float)_progress/_ms.length; }

  public GLMGridStatus(Key taskey, ValueArray va, int ccol, int[] xs, double[]l1s, double[]l2s, double[]rs, double[]as, double caseval) {
    _taskey = taskey;           // Capture the args
    _ary = va;                  // VA is large, and already in a Key so make it transient
    _datakey = va._key;         // ... and use the datakey instead when reloading
    _ccol = ccol;
    _xs = xs;
    _caseval = caseval;
    _lambda1s = l1s;
    _lambda2s = l2s;
    _rhos     = rs;
    _alphas   = as;
    _max = l1s.length*l2s.length*rs.length*as.length;
    _ms = new GLMXValidation[_max];
  }
  // Void constructor for serialization
  public GLMGridStatus() {}

  // Work on this task.  Only *this* main thread updates '_working' &
  // '_progress' & '_ms' fields.  Only other web threads update the '_stop'
  // field; web threads also atomically-read the progress&ms fields.
  public void compute() {
    assert _working == true;
    final int O = _alphas.length;
    final int N = _rhos.length*O;
    final int M = _lambda2s.length*N;

    OUTER:
    for( int l1=0; l1<_lambda1s.length; l1++ )
      for( int l2=0; l2<_lambda2s.length; l2++ )
        for( int r=0; r<_rhos.length; r++ )
          for( int a=0; a<_alphas.length; a++) {
            if( _stop ) break OUTER;
            final GLMXValidation m = do_task(l1,l2,r,a); // Do a step; get a model
            // Now update this Status.
            update(_taskey,m,l1*M+l2*N+r*O+a);
            // Fetch over the 'this' all new bits.  Mostly witness updates to
            // _progress and _stop fields.
            UKV.get(_taskey,this);
          }

    // Update _working to 'false' - we have stopped working
    set_working(_taskey,false);
    tryComplete();            // This task is done
  }

  // Update status for a new model.  In a static function, to avoid closing
  // over the 'this' pointer of a GLMGridStatus and thus serializing it as part
  // of the atomic update.
  static void update( Key taskey, final GLMXValidation m, final int idx) {
    new TAtomic<GLMGridStatus>() {
      @Override public GLMGridStatus atomic(GLMGridStatus old) {
        old._ms[idx] = m; old._progress++; return old; }
      @Override public GLMGridStatus alloc() { return new GLMGridStatus(); }
    }.invoke(taskey);
  }

  // Update the _working field atomically.  In a static function, to avoid
  // closing over the 'this' pointer of a GLMGridStatus and thus serializing it
  // as part of the atomic update.
  static void set_working( Key taskey, final boolean working) {
    new TAtomic<GLMGridStatus>() {
      @Override public GLMGridStatus atomic(GLMGridStatus old) {
        old._working = working; return old; }
      @Override public GLMGridStatus alloc() { return new GLMGridStatus(); }
    }.invoke(taskey);
  }

  // ---
  // Do a single step (blocking).
  // In this case, run 1 GLM model.
  private GLMXValidation do_task( int l1, int l2, int rho, int alpha) {
    // Default GLM args.  Binomial, logit, expanded cats, max iter, etc
    GLMParams glmp = new GLMParams();
    glmp._f = GLMSolver.Family.binomial;
    glmp._l = glmp._f.defaultLink;
    glmp._expandCat = true;
    glmp._maxIter = 20;
    glmp._betaEps = GLMSolver.DEFAULT_BETA_EPS;
    glmp._familyArgs = glmp._f.defaultArgs; // no case/weight.  default 0.5 thresh
    glmp._familyArgs[GLMSolver.FAMILY_ARGS_CASE] = _caseval;

    // The 'step' is the folded iterator
    // Break the step into iterations over the various parameters.


    // Always use elastic-net, but set the various parameters
    LSMSolver lsms = LSMSolver.makeElasticNetSolver(_lambda1s[l1], _lambda2s[l2], _rhos[rho], _alphas[alpha]);
    GLMSolver glm = new GLMSolver(lsms, glmp);
    //GLMModel m = glm.xvalidate(_ary, createColumns(),10)[0]; // fixme, it should contain link to crossvalidatoin results and aggreaget info
    return new GLMXValidation(glm.xvalidate(_ary, createColumns(),10),ErrMetric.SUMC);
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

  public Iterable<GLMXValidation> computedModels(){
    Arrays.sort(_ms, new Comparator<GLMXValidation>() {
      @Override
      public int compare(GLMXValidation o1, GLMXValidation o2) {
        if(o1 == null && o2 == null)return 0;
        if(o1 == null)return 1; // drive the nulls to the end
        if(o2 == null)return -1;
        return o1.compareTo(o2);
      }
    });
    final GLMXValidation [] models = _ms;
    int lastIdx = _ms.length;
    for(int i = 0; i < _ms.length; ++i){
      if(models[i] == null){
        lastIdx = i;
        break;
      }
    }
    final int N = lastIdx;
    return new Iterable<GLMSolver.GLMXValidation>() {

      @Override
      public Iterator<GLMXValidation> iterator() {
        return new Iterator<GLMSolver.GLMXValidation>() {
          int _idx = 0;
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public GLMXValidation next() {
            return models[_idx++];
          }

          @Override
          public boolean hasNext() {
            return _idx < N;
          }
        };
      }
    };
  }
  // Convert all models to Json (expensive!)
  public JsonObject toJson() {
    JsonObject j = new JsonObject();
 // sort models according to their performance

    JsonArray arr = new JsonArray();
    for(GLMXValidation m:_ms){
      if(m == null)break;
      arr.add(m.toJson());
    }
    j.add("models", arr);
    return j;
  }
  // Not intended for remote or distributed execution; task control runs on
  // one node.
  public GLMGridStatus invoke( H2ONode sender ) { throw H2O.unimpl(); }
}
