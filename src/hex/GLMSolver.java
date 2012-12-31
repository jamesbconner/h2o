package hex;

import com.google.gson.*;
import hex.RowVecTask.Sampling;
import java.util.ArrayList;
import java.util.Arrays;
import water.*;

public class GLMSolver {
  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;
  private static final double MAX_SQRT = Math.sqrt(Double.MAX_VALUE);

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
  public static final int FAMILY_ARGS_DECISION_THRESHOLD = 2;

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

    public GLMValidation [] _vals;
    public GLMParams _glmParams;
    public String [] _warnings;
    public boolean is_solved() { return _beta != null; }

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
      for( int col : _colIds ) {
        assert ary._cols[col]._min != ary._cols[col]._max : "already skipped constant cols";
        _colNames[i] = ary._cols[col]._name;
        _colOffsets[i+1] = _colOffsets[i];
        if( _glmParams._expandCat && ary._cols[col]._domain != null && ary._cols[col]._domain.length > 0 ) {
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
      int [] colIds = new int [_colNames.length];
      int idx = 0;
        for(int j = 0; j < _colNames.length; ++j)
          for(int i = 0; i < ary._cols.length; ++i)
            if(ary._cols[i]._name.equals(_colNames[j]))
              colIds[idx++] = i;
      if(idx != colIds.length)throw new Error("incompatible dataset");
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
      valTsk.invoke(ary._key);
      GLMValidation val = new GLMValidation();
      val._beta = _beta;
      val._dataKey = ary._key;
      val._s = s;
      val._f = _glmParams._f;
      val._l = _glmParams._l;
      val._n = valTsk._n;
      val._nullDeviance = valTsk._nullDeviance;
      val._deviance = valTsk._deviance;
      val._err = valTsk._err;
      val._cm = valTsk._cm;
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
    public boolean _expandCat;

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
        res.addProperty("threshold",_familyArgs[FAMILY_ARGS_DECISION_THRESHOLD]);
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
    GLMModel res = new GLMModel(ary,colIds,_solver,_glmParams,s);
    GramMatrixTask gtask = null;
    ArrayList<String> warns = new ArrayList();
    long t1 = System.currentTimeMillis();
    while( res._iterations++ < _glmParams._maxIter ) {
      gtask = new GramMatrixTask(res);
      gtask._s = s;
      gtask.invoke(ary._key);
      double [] beta = null;
      for( int i = 0; i < 20; ++i) {
        try {
          beta = _solver.solve(gtask._gram);
          break;
        } catch( RuntimeException e ) {
          if( !e.getMessage().equals("Matrix is not symmetric positive definite.") )
            throw e;
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
    res._warnings = warns.toArray(new String[warns.size()]);
    return res;
  }

  public GLMModel [] xvalidate(ValueArray ary, int [] colIds, int fold) {
    GLMModel [] models = new GLMModel[fold];
    for(int i = 0; i < fold; ++i){
      models[i] = computeGLM(ary, colIds, new Sampling(i, fold, false));
      models[i].validateOn(ary, new Sampling(i, fold, true));
    }
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
    public double [] _beta;
    double [] _familyArgs;
    public double _deviance;
    public double _nullDeviance;
    public double _err;
    public ConfusionMatrix _cm;

    public JsonObject toJson() {
      JsonObject res = new JsonObject();
      res.addProperty("dataset", _dataKey.toString());
      if(_s != null)
        res.addProperty("sampling", _s.toString());
      res.addProperty("nrows", _n);
      res.addProperty("dof", _n-1-_beta.length);
      res.addProperty("resDev", _deviance);
      res.addProperty("nullDev", _nullDeviance);

      if(_cm != null){
        res.addProperty("err", _cm.err());
        res.add("cm", _cm.toJson());
      } else
        res.addProperty("err", _err);
      return res;
    }
    public double AIC() {
      return 2*(_beta.length+1) + _deviance;
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
    ConfusionMatrix _cm;
    double _err;
    long _n;

    public GLMValidationTask(Key aryKey, int [] colIds){
      super(aryKey,colIds);
    }
    @Override protected void init2() {
      if(_f == Family.binomial)
        _cm = new ConfusionMatrix(2);
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

      _deviance += _f.deviance(yr, ym);
      _nullDeviance += _f.deviance(yr, _ymu);
      if(_f == Family.binomial) {
        ym = ym >= _familyArgs[FAMILY_ARGS_DECISION_THRESHOLD]?1:0;
        _cm.add((int)yr,(int)ym);
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
      if(_cm != null)
        _cm.add(other._cm);
      else
        _cm = other._cm;
    }
  }
}
