package water.api;

import hex.*;
import hex.GLMSolver.*;

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
  double [] _ts;
  double [] _alphas;          // Grid search values
  int _xfold;


  // Progress: Count of GLM models computed so far.
  int _progress;
  int _max;

  // The computed GLM models: product of length of lamda1s,lambda2s,rhos,alphas
  Key _ms[];

  // Fraction complete
  float progress() { return (float)_progress/_ms.length; }

  public GLMGridStatus(Key taskey, ValueArray va, int ccol, int[] xs, double[]l1s, double[]l2s, double[]rs, double[]as, double[]thresholds, double caseval, int xfold) {
    _taskey = taskey;           // Capture the args
    _ary = va;                  // VA is large, and already in a Key so make it transient
    _datakey = va._key;         // ... and use the datakey instead when reloading
    _ccol = ccol;
    _xs = xs;
    _caseval = caseval;
    _lambda1s = l1s;
    _lambda2s = l2s;
    _rhos     = rs;
    _ts       = thresholds;
    _alphas   = as;
    _max = l1s.length*l2s.length*rs.length*as.length;
    _ms = new Key[_max];
    _xfold = xfold;
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
            final GLMModel m = do_task(l1,l2,r,a); // Do a step; get a model
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
  static void update( Key taskey, final GLMModel m, final int idx) {
    new TAtomic<GLMGridStatus>() {
      @Override public GLMGridStatus atomic(GLMGridStatus old) {
        if(m.key() == null)m.store();
        old._ms[idx] = m.key(); old._progress++; return old; }
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
  private GLMModel do_task( int l1, int l2, int rho, int alpha) {
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
    int [] colIds = createColumns();
    GLMModel m = glm.computeGLM(_ary, colIds, null);
    if(_xfold <= 1)
      m.validateOn(_ary, null,_ts);
    else
      glm.xvalidate(m,_ary, createColumns(),_xfold,_ts);
    //GLMModel m = glm.xvalidate(_ary, createColumns(),10)[0]; // fixme, it should contain link to crossvalidatoin results and aggreaget info
    return m;
  }
  // ---

  String model_name( int step ) {
    return "Model "+step;
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

  public Iterable<GLMModel> computedModels(){
    Arrays.sort(_ms, new Comparator<Key>() {
      @Override
      public int compare(Key k1, Key k2) {
        Value v1 = null, v2 = null;
        if(k1 != null)v1 = DKV.get(k1);
        if(k2 != null)v2 = DKV.get(k2);
        if(v1 == null && v2 == null)return 0;
        if(v1 == null)return 1; // drive the nulls to the end
        if(v2 == null)return -1;
        GLMModel m1 = new GLMModel().read(new AutoBuffer(v1.get()));
        GLMModel m2 = new GLMModel().read(new AutoBuffer(v2.get()));
        double cval1 = m1._vals[0].AUC(), cval2 = m2._vals[0].AUC();
        if(cval1 == cval2){
          if(m1._vals[0].classError() != null){
            double [] cerr1 = m1._vals[0].classError(), cerr2 = m2._vals[0].classError();
            assert (cerr2 != null && cerr1.length == cerr2.length);
            for(int i = 0; i < cerr1.length; ++i){
              cval1 += cerr1[i];
              cval2 += cerr2[i];
            }
          }
          if(cval1 == cval2){
            cval1 = m1._vals[0].err();
            cval2 = m2._vals[0].err();
          }
        }

        if(cval1 == cval2)return 0;
        return (cval2 < cval1)?-1:1;
      }
    });
    final Key [] keys = _ms;
    int lastIdx = _ms.length;
    for(int i = 0; i < _ms.length; ++i){
      if(keys[i] == null || DKV.get(keys[i]) == null){
        lastIdx = i;
        break;
      }
    }
    final int N = lastIdx;
    return new Iterable<GLMSolver.GLMModel>() {

      @Override
      public Iterator<GLMModel> iterator() {
        return new Iterator<GLMSolver.GLMModel>() {
          int _idx = 0;
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public GLMModel next() {
            return new GLMModel().read(new AutoBuffer(DKV.get(keys[_idx++]).get()));
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
    for(GLMModel m:computedModels()){
      arr.add(m.toJson());
    }
    j.add("models", arr);
    return j;
  }
  // Not intended for remote or distributed execution; task control runs on
  // one node.
  public GLMGridStatus invoke( H2ONode sender ) { throw H2O.unimpl(); }
}
