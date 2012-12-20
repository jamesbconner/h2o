package hex;

import hex.RowVecTask.Sampling;
import init.H2OSerializable;

import java.util.Arrays;

import sun.security.util.PendingException;

import water.*;

import com.google.gson.*;


public class GLMSolver {
  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;
  private static final double MAX_SQRT = Math.sqrt(Double.MAX_VALUE);

  public static enum Link {
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
    binomial(Link.logit,new double[]{1.0,1.0}),
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

  String [] _colNames;
  int [] _colIds;

  LSMSolver _solver;

  Key _trainingDataset;
  Sampling _sampling;
  double [] _beta;
  int _iterations;
  long _time;
  boolean _finished;
  String [] _warnings;
  transient ValueArray _ary;

  int [] _categoricals;
  int [] _colOffsets;
  double [] _normSub;
  double [] _normMul;

  public static class GLMModel implements H2OSerializable {
    Key _dataset;
    Sampling _s;
    boolean _isDone;
    int _iterations;
    long _time;
    LSMSolver _solver;
    transient String [] _colNames; /// TODO should not be transient!
    int [] _colIds;
    int [] _colOffsets;
    int [] _categoricals;
    int [] _numeric;
    double [] _beta;
    double [] _normSub;
    double [] _normMul;

    GLMValidation [] _vals;
    GLMParams _glmParams;
    transient public String [] _warnings; // TODO should not be transient
    public GLMModel(ValueArray ary, int [] colIds, LSMSolver lsm, GLMParams params, Sampling s){
      _dataset = ary._key;
      _colIds = colIds;
      _colNames = new String[colIds.length];
      _solver = lsm;
      _s = s;
      _glmParams = params;
      int i = 0;
      int [] categoricals = new int[colIds.length];
      int [] numeric = new int[colIds.length];
      _colOffsets = new int[colIds.length+1];
      int ncat = 0,nnum = 0;
      for(int col:_colIds){
        _colNames[i] = ary.col_name(col);
        _colOffsets[i+1] = _colOffsets[i];
        if(ary.col_has_enum_domain(col)){
          categoricals[ncat++] = i;
          _colOffsets[i+1] += ary.col_enum_domain_size(col)-1;
        } else
          numeric[nnum++] = i;
        ++i;
      }
      _categoricals = Arrays.copyOf(categoricals, ncat);
      _numeric = Arrays.copyOf(numeric, nnum);
      int fullLen = _colIds.length + _colOffsets[_colIds.length];
      _beta = new double[fullLen];
      Arrays.fill(_beta, Link.values()[_glmParams._l].defaultBeta);
      if(_solver.normalize()){
        _normMul = new double[fullLen];
        _normSub = new double[fullLen];
        // TODO compute histogram and normalization values for categoricals
        Arrays.fill(_normMul, 1.0);
        Arrays.fill(_normSub, 0.0);
        i = 0;
        for(int j = 0; j < colIds.length-1;++j){
          int col = colIds[j];
          if(!ary.col_has_enum_domain(col)){
            int idx = i + _colOffsets[i];
            _normSub[idx] = ary.col_mean(col);
            _normMul[idx] = 1.0/ary.col_sigma(col);
          }
        }
      }
    }

    public void validateOn(ValueArray ary, Sampling s){
      int [] colIds = new int [_colNames.length];
      int idx = 0;
        for(int j = 0; j < ary.num_cols(); ++j)
          for(int i = 0; i < ary.num_cols(); ++i)
            if(ary.col_name(i).equals(_colNames[j]))
              colIds[idx++] = i;
      if(idx != colIds.length)throw new Error("incompatible dataset");
      GLMValidatoinTask val = new GLMValidatoinTask(ary._key,colIds);
      val._colOffsets = _colOffsets;
      val._categoricals = _categoricals;
      val._numeric = _numeric;
      val._normMul = _normMul;
      val._normSub = _normSub;
      val._val = new GLMValidation();
      val._val._dataKey = ary._key;
      val._val._s = s;
      val._val._f = _glmParams._f;
      val._val._l = _glmParams._l;
      val._val._beta = _beta;
      val._val._familyArgs= _glmParams._familyArgs;
      val.invoke(ary._key);
      if(_vals == null)
        _vals = new GLMValidation[]{val._val};
      else {
        int n = _vals.length;
        _vals = Arrays.copyOf(_vals, n+1);
        _vals[n] = val._val;
      }
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("time", _time);
      res.addProperty("isDone", _isDone);
      res.addProperty("dataset", _dataset.toString());
      JsonArray coefs = new JsonArray();
      ValueArray ary = (ValueArray)DKV.get(_dataset);
      for(int i = 0; i < _colIds.length-1; ++i){
        int col = _colIds[i];
        if(ary.col_has_enum_domain(col)){
          String [] dom = ary.col_enum_domain(col);
          for(int j = 0; j < dom.length; ++j){
            JsonObject c = new JsonObject();
            c.addProperty("name", _colNames[i] + "." + dom[j]);
            c.addProperty("value", _beta[_colOffsets[i]+i+j]);
            coefs.add(c);
          }
        } else {
          JsonObject c = new JsonObject();
          c.addProperty("name", _colNames[i]);
          c.addProperty("value", _beta[_colOffsets[i]+i]);
          coefs.add(c);
        }
      }
      JsonObject c = new JsonObject();
      c.addProperty("name", "Intercept");
      c.addProperty("value", _beta[_beta.length-1]);
      coefs.add(c);

      res.add("coefficients", coefs);
      res.add("LSMParams",_solver.toJson());
      res.add("GLMParams",_glmParams.toJson());
      res.addProperty("iterations", _iterations);
      if(_vals != null){
        JsonArray vals = new JsonArray();
        for(GLMValidation v:_vals)
          vals.add(v.toJson());
        res.add("validations", vals);
      }
      return res;
    }
  }

  public static class GLMParams implements H2OSerializable {
    public int _f = Family.gaussian.ordinal();
    public int _l;
    public double [] _familyArgs;
    public double _betaEps;
    public int _maxIter;

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("family", Family.values()[_f].name());
      res.addProperty("link", Link.values()[_l].name());
      res.addProperty("betaEps", _betaEps);
      res.addProperty("maxIter", _maxIter);
      if(_familyArgs != null){
        assert _f == Family.binomial.ordinal();
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

  public static final double MAX_LAMBDA = 10;
  public GLMModel computeGLM(ValueArray ary, int [] colIds, Sampling s){
    GLMModel res = new GLMModel(ary,colIds,_solver,_glmParams,s);
    GramMatrixTask gtask = null;
    long t1 = System.currentTimeMillis();
    while(res._iterations++ < _glmParams._maxIter){
      gtask = new GramMatrixTask(res);
      gtask._s = s;
      gtask.invoke(ary._key);
      double [] beta = null;
      for(int i = 0; i < 100; ++i){
        try{
          beta = _solver.solve(gtask._gram);
          break;
        } catch (Exception e){
          _solver._penalty = Math.max(_solver._penalty, LSMSolver.L2_PENALTY);
          _solver._lambda = Math.max(_solver._lambda*10, 1e-8);
        }
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
    return res;
  }

  public GLMModel [] xvalidate(ValueArray ary, int [] colIds, int fold){
    GLMModel [] models = new GLMModel[fold];
    for(int i = 0; i < fold; ++i){
      models[i] = computeGLM(ary, colIds, new Sampling(i, fold, false));
      models[i].validateOn(ary, new Sampling(i, fold, true));
    }
    return models;
  }

  public static class GramMatrixTask extends RowVecTask {
    int _f;
    int _l;
    double [] _beta;
    double [] _familyArgs;
    int [] _raw;
    double [] _normSub;
    double [] _normMul;
    transient Link _link;
    transient Family _family;

    GramMatrix _gram;

    public GramMatrixTask(GLMModel m){
      super(new HexDataFrame(m._dataset,m._colIds));
      _f = m._glmParams._f;
      _l = m._glmParams._l;
      _beta = m._beta;
      _familyArgs = m._glmParams._familyArgs;
      _normSub = m._normSub;
      _normMul = m._normMul;
      _colOffsets = m._colOffsets;
      _categoricals = m._categoricals;
      _numeric = m._numeric;
    }

    int rowId;
    public void processRow(double [] x, int [] indexes){
      double y = x[x.length-1];
      // set the intercept
      double w = 1;
      if(_family == Family.binomial){
        if(y == _familyArgs[FAMILY_ARGS_CASE]){
          y = 1.0;
          w = _familyArgs[FAMILY_ARGS_WEIGHT];
        } else
          y = 0;
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
    protected void init(HexDataFrame.ChunkData data){
      _gram = new GramMatrix(_beta.length);
      _link = Link.values()[_l];
      _family = Family.values()[_f];
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GramMatrixTask other = (GramMatrixTask)drt;
      if(_gram != null)_gram.add(other._gram);
      else _gram = other._gram;
    }

  }

  public static final class ConfusionMatrix implements H2OSerializable {
    long [][] _arr;
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

  public static class GLMValidation implements H2OSerializable {
    int _l,_f;
    Key _dataKey;
    Sampling _s;
    long _n;
    double [] _beta;
    double [] _familyArgs;
    double _deviance;
    double _nullDeviance;
    double _err;
    double _ymu; // null hypothesis value
    ConfusionMatrix _cm;

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
  }
  public static class GLMValidatoinTask extends RowVecTask {
    transient Link _link;
    transient Family _family;
    GLMValidation _val;

    public GLMValidatoinTask(Key aryKey, int [] colIds){
      super(new HexDataFrame(aryKey,colIds));
    }
    @Override protected void init(HexDataFrame.ChunkData data){
      _link = Link.values()[_val._l];
      _family = Family.values()[_val._f];
      if(_family == Family.binomial)
        _val._cm = new ConfusionMatrix(2);
    }

    @Override
    void processRow(double[] x, int[] indexes) {
      ++_val._n;
      double yr = x[x.length-1];
      x[x.length-1] = 1.0;
      double ym = 0;
      for(int i = 0; i < x.length; ++i)
        ym += _val._beta[indexes[i]] * x[i];
      ym = _link.linkInv(ym);

      if(_family == Family.binomial)
        yr = yr == _val._familyArgs[FAMILY_ARGS_CASE]?1:0;

      _val._deviance += _family.deviance(yr, ym);
      _val._nullDeviance += _family.deviance(yr, _val._ymu);
      if(_family == Family.binomial) {
        ym = ym >= _val._familyArgs[FAMILY_ARGS_DECISION_THRESHOLD]?1:0;
        _val._cm.add((int)yr,(int)ym);
      } else
        _val._err += (ym - yr)*(ym - yr);
    }

    @Override
    public void reduce(DRemoteTask drt) {
      GLMValidatoinTask other = (GLMValidatoinTask)drt;
      _val._n += other._val._n;
      if(_val == null)
        _val = other._val;
      else {
        _val._nullDeviance += other._val._nullDeviance;
        _val._deviance += other._val._deviance;
        _val._err += other._val._err;
        if(_val._cm != null){
          _val._cm.add(other._val._cm);
        } else
          _val._cm = other._val._cm;
      }
    }
  }
}






