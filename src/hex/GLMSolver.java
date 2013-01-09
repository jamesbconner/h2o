package hex;

import hex.RowVecTask.Sampling;

import java.util.*;

import water.*;

import com.google.gson.*;

public class GLMSolver {
  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;
  private static final double MAX_SQRT = Math.sqrt(Double.MAX_VALUE);

  public static class GLMException extends RuntimeException {
    public GLMException(String msg){super(msg);}
  }

  public static enum Link {
    familyDefault(0),
    identity(0),
    logit(0),
    log(0),
//    probit(0),
//    cauchit(0),
//    cloglog(0),
//    sqrt(0),
    inverse(1.0),
//    oneOverMu2(0);
    ;
    public final double defaultBeta;

    Link(double b){defaultBeta = b;}

    public final double link(double x){
      switch(this){
      case identity:
        return x;
      case logit:
        assert 0 <= x && x <= 1;
        return Math.log(x/(1 - x));
      case log:
        return Math.log(x);
      case inverse:
        return 1/x;
      default:
        throw new Error("unsupported link function id  " + this);
      }
    }

    public final double linkInv(double x){
      switch(this){
      case identity:
        return x;
      case logit:
        return 1.0 / (Math.exp(-x) + 1.0);
      case log:
        return Math.exp(x);
      case inverse:
        return 1/x;
      default:
        throw new Error("unexpected link function id  " + this);
      }
    }

    public final double linkDeriv(double x){
      switch(this){
        case identity:
          return 1;
        case logit:
          if( x == 1 || x == 0 ) return MAX_SQRT;
          return 1 / (x * (1 - x));
        case log:
          return (x == 0)?MAX_SQRT:1/x;
        case inverse:
          return -1/(x*x);
        default:
          throw new Error("unexpected link function id  " + this);
      }
    }
  }


  // helper function
  static final double y_log_y(double y, double mu){
    mu = Math.max(Double.MIN_NORMAL, mu);
    return (y != 0) ? (y * Math.log(y/mu)) : 0;
  }

  // supported families
  public static enum Family {
    gaussian(Link.identity,null),
    binomial(Link.logit,new double[]{Double.NaN,1.0,0.5}),
    poisson(Link.log,null),
    gamma(Link.inverse,null);
    public final Link defaultLink;
    public final double [] defaultArgs;
    Family(Link l, double [] d){defaultLink = l; defaultArgs = d;}

    public double variance(double mu){
      switch(this){
      case gaussian:
        return 1;
      case binomial:
        assert 0 <= mu && mu <= 1:"unexpected mu:" + mu;
        return mu*(1-mu);
      case poisson:
        return mu;
      case gamma:
        return mu*mu;
      default:
        throw new Error("unknown family Id " + this);
      }
    }

    /**
     * Per family deviance computation.
     *
     * @param family
     * @param yr
     * @param ym
     * @return
     */
    public double deviance(double yr, double ym){
      switch(this){
      case gaussian:
        return (yr - ym)*(yr - ym);
      case binomial:
        return 2*((y_log_y(yr, ym)) + y_log_y(1-yr, 1-ym));
        //return -2*(yr * ym - Math.log(1 + Math.exp(ym)));
      case poisson:
        //ym = Math.exp(ym);
        if(yr == 0)return 2*ym;
        return 2*((yr * Math.log(yr/ym)) - (yr - ym));
      case gamma:
        if(yr == 0)return -2;
        return -2*(Math.log(yr/ym) - (yr - ym)/ym);
      default:
        throw new Error("unknown family Id " + this);
      }
    }
  }

  public static final int FAMILY_ARGS_CASE = 0;
  public static final int FAMILY_ARGS_WEIGHT = 1;

  public static final double [] DEFAULT_THRESHOLDS = new double [] {0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9};


  int [] _colIds;

  LSMSolver _solver;

  Key _trainingDataset;
  Sampling _sampling;
  double [] _beta;
  int _iterations;
  boolean _finished;
  transient ValueArray _ary;

  int [] _categoricals;
  int [] _colOffsets;
  double [] _normSub;
  double [] _normMul;

  public static class GLMModel extends Iced {
    public Key _dataset;
    Key _key;
    Sampling _s;
    boolean _isDone;            // Model is "being worked on" or "is stable"
    public int _iterations;
    public long _time;
    public LSMSolver _solver;
    String [] _colNames;
    int [] _colIds;
    int [] _colOffsets;
    int [] _categoricals;
    int [] _numeric;
    double [] _beta;
    double [] _normSub;
    double [] _normMul;

    public GLMParams _glmParams;
    public String [] _warnings;
    public GLMValidation [] _vals;

    public boolean is_solved() { return _beta != null; }


    public static final String KEY_PREFIX = "__GLMModel_";


    public static final Key makeKey() {
      return Key.make(KEY_PREFIX + Key.make());
    }

    public final void store() {
      if(_key == null)
        _key = makeKey();
      UKV.put(_key, this);
    }

    public final Key key(){
      return _key;
    }
    public GLMModel(){}

    public GLMModel(ValueArray ary, int [] colIds, LSMSolver lsm, GLMParams params, Sampling s) {
      _dataset = ary._key;

      ArrayList<Integer> validCols = new ArrayList<Integer>();
      for( int col : colIds ) {
        if(ary._cols[col]._max != ary._cols[col]._min)
          validCols.add(col);
        else
          System.out.println("ignoring constant column " + col);
      }
      colIds = null;
      _colIds = new int [validCols.size()];
      int cid = 0;
      for(int c:validCols){
        _colIds[cid++] = c;
      }
      _colNames = new String[_colIds.length];
      _solver = lsm;
      _s = s;
      _glmParams = params;

      int i = 0;
      int [] categoricals = new int[_colIds.length];
      int [] numeric = new int[_colIds.length];
      _colOffsets = new int[_colIds.length+1];
      int ncat = 0,nnum = 0;
      for( int j = 0; j < _colIds.length;++j ) {
        int col = _colIds[j];
        assert ary._cols[col]._min != ary._cols[col]._max : "already skipped constant cols";
        _colNames[i] = ary._cols[col]._name;
        _colOffsets[i+1] = _colOffsets[i];
        if(j != (_colIds.length-1) && _glmParams._expandCat && ary._cols[col]._domain != null && ary._cols[col]._domain.length > 0 ) {
          categoricals[ncat++] = i;
          _colOffsets[i+1] += ary._cols[col]._domain.length;
        } else
          numeric[nnum++] = i;
        ++i;
      }



      _categoricals = Arrays.copyOf(categoricals, ncat);
      _numeric = Arrays.copyOf(numeric, nnum);

      int fullLen = _colIds.length + _colOffsets[_colIds.length];
      _beta = new double[fullLen];
      Arrays.fill(_beta, _glmParams._l.defaultBeta);
      if(_solver.normalize()){
        _normMul = new double[fullLen];
        _normSub = new double[fullLen];
        // TODO compute histogram and normalization values for categoricals
        Arrays.fill(_normMul, 1.0);
        Arrays.fill(_normSub, 0.0);
        i = 0;
        for(int j = 0; j < _colIds.length-1;++j){
          int col = _colIds[j];
          if(!(ary._cols[col]._domain != null && ary._cols[col]._domain.length > 0)) {
            int idx = j + _colOffsets[j];
            _normSub[idx] = ary._cols[col]._mean;
            if(ary._cols[col]._sigma != 0)
              _normMul[idx] = 1.0/ary._cols[col]._sigma;
          } else if(_glmParams._expandCat){

          }
        }
      }
    }


    public void validateOn(ValueArray ary, Sampling s){
      validateOn(ary, s,DEFAULT_THRESHOLDS);
    }
    public void validateOn(ValueArray ary, Sampling s, double [] thresholds){
      int [] colIds = new int [_colNames.length];
      int idx = 0;
        for(int j = 0; j < _colNames.length; ++j)
          for(int i = 0; i < ary._cols.length; ++i)
            if(ary._cols[i]._name.equals(_colNames[j]))
              colIds[idx++] = i;
      if(idx != colIds.length)throw new GLMException("incompatible dataset");
      GLMValidationTask valTsk = new GLMValidationTask(ary._key,colIds);
      valTsk._colOffsets = _colOffsets;
      valTsk._categoricals = _categoricals;
      valTsk._numeric = _numeric;
      valTsk._normMul = _normMul;
      valTsk._normSub = _normSub;
      valTsk._s = s;
      valTsk._f = _glmParams._f;
      valTsk._l = _glmParams._l;
      valTsk._beta = _beta;
      valTsk._familyArgs= _glmParams._familyArgs;
      valTsk._thresholds = thresholds;
      valTsk.invoke(ary._key);
      GLMValidation val = new GLMValidation(valTsk);
//      val._beta = _beta;
      val._dataKey = ary._key;
      val._s = s;
      val._f = _glmParams._f;
      val._l = _glmParams._l;

      if(_vals == null)
        _vals = new GLMValidation[]{val};
      else {
        int n = _vals.length;
        _vals = Arrays.copyOf(_vals, n+1);
        _vals[n] = val;
      }
    }



    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("time", _time);
      res.addProperty("isDone", _isDone);
      res.addProperty("dataset", _dataset.toString());
      if( _warnings != null ) {
        JsonArray warnings = new JsonArray();
        for( String w : _warnings )
          warnings.add(new JsonPrimitive(w));
        res.add("warnings", warnings);
      }
      if( _beta == null ) return res; // Not solved!
      JsonObject coefs = new JsonObject();
      ValueArray ary = ValueArray.value(_dataset);
      double norm = 0.0;        // Reverse any normalization on the intercept
      for( int i = 0; i < _colIds.length-1; ++i ) {
        int col = _colIds[i];
        int ii = _colOffsets[i]+i;
        if(_glmParams._expandCat && (ary._cols[col]._domain != null && ary._cols[col]._domain.length != 0)){
          String [] dom = ary._cols[col]._domain;
          for(int j = 0; j < dom.length; ++j)
            coefs.addProperty(_colNames[i] + "." + dom[j],_beta[ii+j]);
        } else {
          double b = _beta[ii]; // Compute cooeficients
          if( _normSub != null ) { // Remove any normalization
            b *= _normMul[ii];
            norm += b*_normSub[ii];
          }
          coefs.addProperty(_colNames[i],b);
        }
      }
      double icpt = _beta[_beta.length-1];
      if( _normSub != null ) icpt -= norm;
      coefs.addProperty("Intercept",icpt);
      res.add("coefficients", coefs);
      res.add("LSMParams",_solver.toJson());
      res.add("GLMParams",_glmParams.toJson());
      res.addProperty("iterations", _iterations);
      if(_vals != null) {
        JsonArray vals = new JsonArray();
        for(GLMValidation v:_vals)
          vals.add(v.toJson());
        res.add("validations", vals);
      }
      return res;
    }
  }

  public static class GLMParams extends Iced {
    public Family _f = Family.gaussian;
    public Link _l;
    public double [] _familyArgs;
    public double _betaEps;
    public int _maxIter;
    public boolean _expandCat = true; // Always on for now

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("family", _f.toString());
      res.addProperty("link", _l.toString());
      res.addProperty("betaEps", _betaEps);
      res.addProperty("maxIter", _maxIter);
      if(_familyArgs != null){
        assert _f == Family.binomial;
        res.addProperty("caseVal",_familyArgs[FAMILY_ARGS_CASE]);
        res.addProperty("weight",_familyArgs[FAMILY_ARGS_WEIGHT]);
      }
      return res;
    }
  }

  GLMParams _glmParams;

  public GLMSolver(LSMSolver lsm, GLMParams glmParams){
    _solver = lsm;
    _glmParams = glmParams;
  }

  public GLMModel computeGLM(ValueArray ary, int [] colIds, Sampling s) {
    // check range of response variable (y)
    if(_glmParams._f == Family.binomial && Double.isNaN(_glmParams._familyArgs[FAMILY_ARGS_CASE])){
      int ycol = colIds[colIds.length-1];
      if(ary._cols[ycol]._max > 1 || ary._cols[ycol]._min < 0)
        throw new GLMException("Response variable out of range, 0:1 range is required, got " + ary._cols[ycol]._min + ":" + ary._cols[ycol]._max);
    }
    GLMModel res = new GLMModel(ary,colIds,_solver,_glmParams,s);
    GramMatrixTask gtask = null;
    ArrayList<String> warns = new ArrayList();
    long t1 = System.currentTimeMillis();
 OUTER:
    while( res._iterations++ < _glmParams._maxIter ) {
      gtask = new GramMatrixTask(res);
      gtask._s = s;
      gtask.invoke(ary._key);
      double [] beta = null;
      for( int i = 0; i < 20; ++i) {
        try {
          beta = _solver.solve(gtask._gram);
          for(double d:beta)if(Double.isNaN(d) || Double.isInfinite(d)){
            warns.add("Failed to converge!");
            break OUTER;
          }
          break;
        } catch( RuntimeException e ) {
          if( !e.getMessage().equals("Matrix is not symmetric positive definite.") )
            throw e;
          if(gtask._gram.hasNaNsOrInfs()){
            warns.add("Failed to converge!");
            break OUTER;
          }
          System.out.println(gtask._gram);
          switch(_solver._penalty) {
          case NONE:
            _solver._penalty = LSMSolver.Norm.L2;
            _solver._lambda = 1e-8;
            warns.add("Gram matrix is not SPD; adding L2 penalty of "+_solver._lambda);
            break;
          case L2:
            _solver._lambda *= 10;
            warns.add("Gram matrix is still not SPD; increasing L2 to penalty "+_solver._lambda);
            break;
          case L1:
            _solver._penalty = LSMSolver.Norm.ELASTIC;
            _solver._lambda2 = 1e-8;
            warns.add("Gram matrix is not SPD; adding L2 penalty of "+_solver._lambda2);
            break;
          case ELASTIC:
            _solver._lambda2 *= 10;
            warns.add("Gram matrix is not SPD; increasing L2 penalty to "+_solver._lambda2);
            break;
          default:
            throw new IllegalArgumentException();
          }
        }
      }
      if( beta == null ) {      // Failed after 20 goes?
        warns.add("Cannot solve");
        gtask = null;
        break;
      }
      double diff = 0.0;
      for(int i = 0; i < gtask._beta.length; ++i)
        diff = Math.max(diff, Math.abs(beta[i] - gtask._beta[i]));
      res._beta = beta;
      res._time = System.currentTimeMillis() - t1;
      if(diff < _glmParams._betaEps)
        break;
    }
    res._beta = (gtask != null)?gtask._beta:null;
    res._isDone = true;
    if(!warns.isEmpty())
      res._warnings = warns.toArray(new String[warns.size()]);
    return res;
  }

//  public static final double DTHRESHOLD_STEP = 0.01;
//  public static final double DTHRESHOLD_LB = 0.01;
//  public static final double DTHRESHOLD_UB = 0.99;
  //public static final int N_THRESHOLD_POINTS = 1 + (int)((DTHRESHOLD_UB - DTHRESHOLD_LB)/DTHRESHOLD_STEP);

//  public static int getThresholdIdx(double t){
//    if( 0 > t || t > 1) throw new Error("illegal threshold " + t);
//    if(t <= DTHRESHOLD_LB) return 0;
//    if(t >= DTHRESHOLD_UB) return N_THRESHOLD_POINTS-1;
//    return (int) Math.round((t - DTHRESHOLD_LB)/DTHRESHOLD_STEP);
//  }
//
//  public static double getThresholdValue(int idx){
//    return DTHRESHOLD_LB + idx*DTHRESHOLD_STEP;
//  }

//  public static final double DEFAULT_THRESHOLD_VAL = 0.5;
//  public static final int DEFAULT_THRESHOLD_IDX = getThresholdIdx(DEFAULT_THRESHOLD_VAL);


  public enum ErrMetric {
    MAXC,
    SUMC,
    TOTAL;

    public double computeErr(ConfusionMatrix cm){
      double [] cerr = cm.classErr();
      double res = 0;
      switch(this){
      case MAXC:
         res = cerr[0];
        for(double d:cerr)if(d > res)res = d;
        break;
      case SUMC:
        for(double d:cerr)res += d;
        break;
      case TOTAL:
        res = cm.err();
        break;
      default:
        throw new Error("unexpected err metric " + this);
      }
      return res;
    }

  }

  public static class GLMXValidation extends GLMValidation implements Comparable<GLMXValidation>{


    Key [] _modelKeys;
    boolean _compareByAUC = true;

    public Key [] modelKeys(){
      return _modelKeys;
    }

    public Iterable<GLMModel> models(){
      final Key [] keys = _modelKeys;
      return new Iterable<GLMModel> (){
        int idx;
        @Override
        public Iterator<GLMModel> iterator() {
          return new Iterator<GLMSolver.GLMModel>() {
            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }

            @Override
            public GLMModel next() {
              if(idx == keys.length) throw new NoSuchElementException();
              return new GLMModel().read(new AutoBuffer(DKV.get(keys[idx++]).get()));
            }
              @Override
            public boolean hasNext() {
              return idx < keys.length;
            }
          };
        }
      };
    }

    public GLMXValidation(GLMModel [] models, ErrMetric m, double [] thresholds) {
      _errMetric = m;
      if(models[0]._vals[0]._cm != null){
        int nthresholds = models[0]._vals[0]._cm.length;
        _cm = new ConfusionMatrix[nthresholds];
        for(int t = 0; t < nthresholds; ++t)
          _cm[t] = models[0]._vals[0]._cm[t];
        _n += models[0]._vals[0]._n;
        _deviance = models[0]._vals[0]._deviance;
        _nullDeviance = models[0]._vals[0]._nullDeviance;
        for(int i = 1; i < models.length; ++i){
          _n += models[i]._vals[0]._n;
          _deviance += models[0]._vals[0]._deviance;
          _nullDeviance += models[0]._vals[0]._nullDeviance;
          for(int t = 0; t < nthresholds; ++t)
            _cm[t].add(models[i]._vals[0]._cm[t]);
        }
        _thresholds = thresholds;
        computeBestThreshold(m);
        computeAUC();
      } else {
        for(GLMModel xm:models){
          _n += xm._vals[0]._n;
          _deviance += xm._vals[0]._deviance;
          _nullDeviance += xm._vals[0]._nullDeviance;
          _err += xm._vals[0]._err;
        }
      }
      _aic = 2*(models[0]._beta.length+1) + _deviance;
      _dof = _n - models[0]._beta.length - 1;
      _modelKeys = new Key[models.length];
      int i = 0;
      for(GLMModel xm:models){
        if(xm.key() == null)xm.store();
        _modelKeys[i++] = xm.key();
      }
    }



    public double errM(){
      return _errMetric.computeErr(_cm[_tid]);
    }

    public ConfusionMatrix cm() {
      return _cm[_tid];
    }
    public JsonObject toJson(){
      JsonObject res = super.toJson();

      return res;
    }

    @Override
    public int compareTo(GLMXValidation o) {
      double x,y;
      if(_compareByAUC){
        x = -AUC();
        y = -o.AUC();
      } else {
        x = _errMetric.computeErr(_cm[_tid]);
        y = _errMetric.computeErr(o._cm[o._tid]);
      }
      if(x < y)return -1;
      if(x > y) return 1;
      return 0;
    }
  }

  public GLMModel [] xvalidate(GLMModel m, ValueArray ary, int [] colIds, int fold){
    return xvalidate(m, ary, colIds, fold,DEFAULT_THRESHOLDS);
  }
  public GLMModel [] xvalidate(GLMModel m, ValueArray ary, int [] colIds, int fold, double [] thresholds) {
    GLMModel [] models = new GLMModel[fold];
    for(int i = 0; i < fold; ++i){
      models[i] = computeGLM(ary, colIds, new Sampling(i, fold, false));
      models[i].validateOn(ary, new Sampling(i, fold, true),thresholds);
    }
    m._vals = new GLMValidation[]{new GLMXValidation(models, ErrMetric.SUMC,thresholds)};
    return models;
  }

  public static class GramMatrixTask extends RowVecTask {
    Family _family;
    Link _link;
    double [] _beta;
    double [] _familyArgs;
    GramMatrix _gram;

    public GramMatrixTask(GLMModel m){
      super(m._dataset,m._colIds);
      _family = m._glmParams._f;
      _link = m._glmParams._l;
      _beta = m._beta;
      _familyArgs = m._glmParams._familyArgs;
      _normSub = m._normSub;
      _normMul = m._normMul;
      _colOffsets = m._colOffsets;
      _categoricals = m._categoricals;
      _numeric = m._numeric;
    }

    public void processRow(double [] x, int [] indexes){
      double y = x[x.length-1];
      // set the intercept
      double w = 1;
      // For binomials over enum response variables, allow one case to be
      // "success" and all other cases to be "fails".
      if( _family == Family.binomial &&
          !Double.isNaN(_familyArgs[FAMILY_ARGS_CASE]) ) {
        if( _familyArgs[FAMILY_ARGS_CASE] == y ) {
          y = 1.0;              // Success case; map to 1
          w = _familyArgs[FAMILY_ARGS_WEIGHT];
        } else {
          y = 0;                // Fail case; map to 0
        }
      }

      x[x.length-1] = 1.0; // constant (Intercept)
      double gmu = 0;
      for(int i = 0; i < x.length; ++i) {
        gmu += x[i] * _beta[indexes[i]];
      }
      // get the inverse to get estimate of p(Y=1|X) according to previous model
      double mu = _link.linkInv(gmu);
      double dgmu = _link.linkDeriv(mu);
      y = gmu + (y - mu) * dgmu; // z = y approx by Taylor
                                               // expansion at the point of our
                                               // estimate (mu), done to avoid
                                               // log(0),log(1)
      // Step 2
      double vary = _family.variance(mu); // variance of y according to our model

      // compute the weights (inverse of variance of z)
      double var = dgmu * dgmu * vary;
      // Apply the weight. We want each data point to have weight of inverse of
      // the variance of y at this point.
      // Since we compute x'x, we take sqrt(w) and apply it to both x and y
      // (we also compute X*y)
      w = Math.sqrt(w/var);
      for(int i = 0; i < x.length; ++i)
        x[i] *= w;
      _gram.addRow(x, indexes, y * w);
    }

    @Override
    protected void init2(){
      _gram = new GramMatrix(_beta.length);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GramMatrixTask other = (GramMatrixTask)drt;
      if(_gram != null)_gram.add(other._gram);
      else _gram = other._gram;
    }
  }

  public static final class ConfusionMatrix extends Iced {
    public long [][] _arr;
    long _n;
    double _threshold;

    public ConfusionMatrix(int n){
      _arr = new long[n][n];
    }
    public void add(int i, int j){
      add(i, j, 1);
    }
    public void add(int i, int j, int c){
      _arr[i][j] += c;
      _n += c;
    }

    public final double [] classErr(){
      double [] res = new double[_arr.length];
      for(int i = 0; i < res.length; ++i)
        res[i] = classErr(i);
      return res;
    }
    public final int size() {return _arr.length;}

    public final double classErr(int c){
      long s = 0;
      for( long x : _arr[c] )
        s += x;
      if( s==0 ) return 0.0;    // Either 0 or NaN, but 0 is nicer
      return (double)(s-_arr[c][c])/s;
    }

    public double err(){
      long err = _n;
      for(int i = 0; i < _arr.length;++i){
        err -= _arr[i][i];
      }
      return (double)err/_n;
    }

    public void add(ConfusionMatrix other){
      _n += other._n;
      for(int i = 0; i < _arr.length; ++i)
        for(int j = 0; j < _arr.length; ++j)
          _arr[i][j] += other._arr[i][j];
    }

    public JsonArray toJson(){
      JsonArray res = new JsonArray();
      JsonArray header = new JsonArray();
      header.add(new JsonPrimitive("Actual / Predicted"));
      for(int i = 0; i < _arr.length;++i)
        header.add(new JsonPrimitive("class " + i));
      header.add(new JsonPrimitive("Error"));
      res.add(header);
      for(int i = 0; i < _arr.length; ++i){
        JsonArray row = new JsonArray();
        row.add(new JsonPrimitive("class " + i));
        long s = 0;
        for(int j = 0; j < _arr.length; ++j){
          s += _arr[i][j];
          row.add(new JsonPrimitive(_arr[i][j]));
        }
        double err = s - _arr[i][i];
        err /= s;
        row.add(new JsonPrimitive(err));
        res.add(row);
      }
      JsonArray totals = new JsonArray();
      totals.add(new JsonPrimitive("Totals"));
      long S = 0;
      long DS = 0;
      for(int i = 0; i < _arr.length; ++i){
        long s = 0;
        for(int j = 0; j < _arr.length; ++j)
          s += _arr[j][i];
        totals.add(new JsonPrimitive(s));
        S += s;
        DS += _arr[i][i];
      }
      double err = (S - DS)/(double)S;
      totals.add(new JsonPrimitive(err));
      res.add(totals);
      return res;
    }
  }

  public static class GLMValidation extends Iced {
    Link _l;
    Family _f;
    Key _dataKey;
    Sampling _s;
    public long _n;
    public double _dof;
    public double _aic;
    public double _deviance;
    public double _nullDeviance;
    public double _err;
    ErrMetric _errMetric = ErrMetric.SUMC;
    double _auc;
    public ConfusionMatrix [] _cm;
    int _tid;
    double [] _thresholds;
    public GLMValidation(){}


    public GLMValidation(GLMValidationTask tsk){
      _n = tsk._n;
      _nullDeviance = tsk._nullDeviance;
      _deviance = tsk._deviance;
      _err = tsk._err;
      _cm = tsk._cm;
      _dof = _n-1-tsk._beta.length;
      _aic = 2*(tsk._beta.length+1) + _deviance;
      _thresholds = tsk._thresholds;
      if(_cm != null){
        computeBestThreshold(ErrMetric.SUMC);
        computeAUC();
      }
    }

    public ConfusionMatrix bestCM(){
      if(_cm == null)return null;
      return bestCM(ErrMetric.SUMC);
    }

    public double err() {
      if(_cm != null)return bestCM().err();
      return _err;
    }
    public ConfusionMatrix bestCM(ErrMetric errM){
      computeBestThreshold(errM);
      return _cm[_tid];
    }

    public double bestThreshold() {
      return _thresholds[_tid];
    }
    public void computeBestThreshold(ErrMetric errM){
      double e = errM.computeErr(_cm[0]);
      _tid = 0;
      for(int i = 1; i < _cm.length; ++i){
        double r = errM.computeErr(_cm[i]);
        if(r < e){
          e = r;
          _tid = i;
        }
      }
    }


    double [] err(int c) {
      double [] res = new double[_cm.length];
      for(int i = 0; i < res.length; ++i)
        res[i] = _cm[i].classErr(c);
      return res;
    }

    double err(int c, int threshold) {
      return _cm[threshold].classErr(c);
    }

    public double [] classError() {
      return _cm[_tid].classErr();
    }
    private double trapeziod_area(double x1, double x2, double y1, double y2){
      double base = Math.abs(x1-x2);
      double havg = 0.5*(y1 + y2);
      return base*havg;
    }

    public double AUC(){
      return _auc;
    }

    /**
     * Computes area under the ROC curve.
     * The ROC curve is computed from the confusion matrices (there is one for
     * each computed threshold).  Area under this curve is then computed as a
     * sum of areas of trapezoids formed by each neighboring points.
     *
     * @return estimate of the area under ROC curve of this classifier.
     */
    protected void computeAUC() {
      double auc = 0;           // Area-under-ROC
      double TPR_pre = 1;
      double FPR_pre = 1;

      for(int t = 0; t < _cm.length; ++t){
        double TPR = 1 - _cm[t].classErr(1); // =TP/(TP+FN) = true -positive-rate
        double FPR =     _cm[t].classErr(0); // =FP/(FP+TN) = false-positive-rate
        auc += trapeziod_area(FPR_pre, FPR, TPR_pre, TPR);
        TPR_pre = TPR;
        FPR_pre = FPR;
      }
      auc += trapeziod_area(FPR_pre, 0, TPR_pre, 0);
      _auc = auc;
    }


    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      if(_dataKey != null)
        res.addProperty("dataset", _dataKey.toString());
      else
        res.addProperty("dataset", "");
      if(_s != null)
        res.addProperty("sampling", _s.toString());
      res.addProperty("nrows", _n);
      res.addProperty("dof", _dof);
      res.addProperty("resDev", _deviance);
      res.addProperty("nullDev", _nullDeviance);

      if(_cm != null) {
        double [] err = _cm[_tid].classErr();
        JsonArray arr = new JsonArray();
        for(int i = 0; i < err.length; ++i)
          arr.add(new JsonPrimitive(err[i]));
        res.add("err", arr);
        res.addProperty("threshold", _thresholds[_tid]);
        res.add("cm", _cm[_tid].toJson());
      } else
        res.addProperty("err", _err);
      return res;
    }

    public double AIC() {
      return _aic;
    }
  }
  public static class GLMValidationTask extends RowVecTask {
    Link _l;
    Family _f;
    double _ymu;
    double _deviance;
    double _nullDeviance;
    double [] _beta;
    double [] _familyArgs;
    double [] _thresholds;
    ConfusionMatrix [] _cm;
    double _err;
    long _n;

    public GLMValidationTask(Key aryKey, int [] colIds){
      super(aryKey,colIds);
    }
    @Override protected void init2() {
      if(_f == Family.binomial) {
        _cm = new ConfusionMatrix[_thresholds.length];
        for(int i = 0; i < _thresholds.length; ++i)
          _cm[i] = new ConfusionMatrix(2);
      }
    }

    @Override
    void processRow(double[] x, int[] indexes) {
      ++_n;
      double yr = x[x.length-1];
      x[x.length-1] = 1.0;
      double ym = 0;
      for(int i = 0; i < x.length; ++i)
        ym += _beta[indexes[i]] * x[i];
      ym = _l.linkInv(ym);

      if( _f == Family.binomial &&
          !Double.isNaN(_familyArgs[FAMILY_ARGS_CASE]) )
        yr = yr == _familyArgs[FAMILY_ARGS_CASE]?1:0;
      if(yr < 0 || yr > 1 )
        throw new Error("response variable value out of range: " + yr);
      _deviance += _f.deviance(yr, ym);
      _nullDeviance += _f.deviance(yr, _ymu);
      if(_f == Family.binomial) {
        int i = 0;
        for(double t:_thresholds){
          int p = ym >= t?1:0;
          _cm[i++].add((int)yr,p);
        }
      } else
        _err += (ym - yr)*(ym - yr);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GLMValidationTask other = (GLMValidationTask)drt;
      _n += other._n;
      _nullDeviance += other._nullDeviance;
      _deviance += other._deviance;
      _err += other._err;
      if(_cm != null) {
        for(int i = 0; i < _thresholds.length; ++i)
          _cm[i].add(other._cm[i]);
      } else
        _cm = other._cm;
    }
  }
}
