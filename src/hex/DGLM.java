package hex;

import hex.DLSM.LSMSolver;
import hex.GLMSolver.ConfusionMatrix;
import hex.GLMSolver.ErrMetric;
import hex.NewRowVecTask.DataFrame;
import hex.NewRowVecTask.RowAlg;
import hex.NewRowVecTask.RowAlgFactory;
import hex.RowVecTask.Sampling;

import java.util.*;

import water.*;

import com.google.gson.*;




public abstract class DGLM {

  public static final int DEFAULT_MAX_ITER = 50;
  public static final double DEFAULT_BETA_EPS = 1e-4;
  private static final double MAX_SQRT = Math.sqrt(Double.MAX_VALUE);

  public static class GLMException extends RuntimeException {
    public GLMException(String msg){super(msg);}
  }



  public static class GLMParams extends Iced {
    public Family _family = Family.gaussian;
    public Link _link;
    public double _betaEps = 1e-4;
    public int _maxIter = 50;
    public double _caseVal;
    public double _caseWeight = 1.0;
    public CaseMode _caseMode = CaseMode.none;
    public boolean _reweightGram = true;

    public GLMParams(Family family){this(family,family.defaultLink);}

    public GLMParams(Family family, Link link){
      _family = family;
      _link = link;
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("family", _family.toString());
      res.addProperty("link", _link.toString());
      res.addProperty("betaEps", _betaEps);
      res.addProperty("maxIter", _maxIter);
      if(_caseMode != null && _caseMode != CaseMode.none){
        res.addProperty("caseVal",_caseMode.exp(_caseVal));
        res.addProperty("weight",_caseWeight);
      }
      return res;
    }
  }

  public enum CaseMode {
    none("n/a"),
    lt("<"),
    gt(">"),
    lte("<="),
    gte(">="),
    eq("=");
    final String _str;

    CaseMode(String str){
      _str = str;
    }
    public String toString(){
      return _str;
    }

    public String exp(double v){
      switch(this){
      case none:
        return "n/a";
      default:
        return "x" + _str + v;
      }
    }

    public final boolean isCase(double x, double y){
      switch(this){
      case lt:
        return x < y;
      case gt:
        return x > y;
      case lte:
        return x <= y;
      case gte:
        return x >= y;
      case eq:
        return x == y;
      default:
        assert false;
        return false;
      }
    }
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
    poisson(Link.log,null);
    //gamma(Link.inverse,null);
    public final Link defaultLink;
    public final double [] defaultArgs;
    Family(Link l, double [] d){defaultLink = l; defaultArgs = d;}

    public double weightApproximation(){
      switch(this){
      case gaussian:
        return 1;
      case binomial:
        return 0.25;
      case poisson:
        throw new Error("unimplmented");
//      case gamma:
//        return Double.NaN;
      default:
        throw new Error("unknown family Id " + this);
      }
    }

    public double aic(double dev, long nobs, long dof){
      int betaLen = (int)(nobs - dof);
      switch(this){
      case gaussian:
        return nobs *(Math.log(dev/nobs * 2 *Math.PI)+1)+2 + 2*betaLen;
      case binomial:
        return 2*betaLen + dev;
      case poisson:
        return 2*betaLen + dev;
//      case gamma:
//        return Double.NaN;
      default:
        throw new Error("unknown family Id " + this);
      }
    }
    public double variance(double mu){
      switch(this){
      case gaussian:
        return 1;
      case binomial:
        assert 0 <= mu && mu <= 1:"unexpected mu:" + mu;
        return mu*(1-mu);
      case poisson:
        return mu;
//      case gamma:
//        return mu*mu;
      default:
        throw new Error("unknown family Id " + this);
      }

    }

//    public double weight(double yr, double mu, Link l){
//    public double z(double yr, double mu, Link l){
//      switch(this){
//      case gaussian:
//        assert(false); // no need for this with gaussian!
//        return yr - mu;
//      case binomial:
//        assert 0 <= yr && yr <= 1;
//        double p = l.linkInv(mu);
//        assert 0 <= p && p <= 1;
//        double w = p*(1-p);
//        if(p < 1e-5){
//          p = 0;
//          w = 1e-5;
//        } else if(p > (1-1e-5)){
//          p = 1;
//          w = 1e-5;
//        }
//        return Math.sqrt(w)*(mu + (yr - p)/w);
//      default:
//        throw new Error("unimplemented");
//      }
//    }


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
//      case gamma:
//        if(yr == 0)return -2;
//        return -2*(Math.log(yr/ym) - (yr - ym)/ym);
      default:
        throw new Error("unknown family Id " + this);
      }
    }
  }


  public static class GLMModel extends Iced {
    Key       _trainingData;
    String [] _colNames;
    double [] _beta;
    boolean   _converged;
    GLMParams _glmp;
    String [] _warnings;
    Key       _key;
    GLMValidation []    _vals;

    public GLMModel(){}
    public GLMModel(DataFrame data, double [] beta, GLMParams glmp, boolean converged, String [] warnings){
      _trainingData = data._aryKey;
      _colNames = data.colNames();
      _beta = beta;
      _converged = converged;
      _glmp = glmp;
      _warnings = warnings;
    }


    public boolean is_solved() { return _beta != null; }
    public static final String KEY_PREFIX = "__GLMModel_";

    public static final Key makeKey() {
      return Key.make(KEY_PREFIX + Key.make());
    }

    public final void store() {
      if(_key == null){
        _key = makeKey();
        if(_vals != null)for(GLMValidation v:_vals)
          v._modelKey = _key;
      }
      UKV.put(_key, this);
    }
    public boolean isCompatible(ValueArray ary, int [] colIds){
      if( colIds == null ) colIds = new int [_colNames.length];
      else assert colIds.length == _colNames.length;
      for(int j = 0; j < _colNames.length; ++j) {
        colIds[j] = -1;
        for(int i = 0; i < ary._cols.length; ++i) {
          if(ary._cols[i]._name.equals(_colNames[j])) {
            colIds[j] = i;
            break;
          }
        }
        if( colIds[j] == -1 ) return false;
      }
      return true;
    }

    public GLMValidation validateOn(ValueArray ary, HexSampling s){
      return null;
    }

    public JsonObject toJson(){
      JsonObject res = new JsonObject();
      res.addProperty("trainingDataset", _trainingData.toString());
      JsonArray colnames = new JsonArray();
      for(String s:_colNames)colnames.add(new JsonPrimitive(s));
      res.add("colNames", colnames);
      JsonArray beta = new JsonArray();
      for(double d:_beta)
        beta.add(new JsonPrimitive(d));
      res.add("coefs",beta);
      res.add("glm_params",_glmp.toJson());
      JsonArray warns = new JsonArray();
      for(String s:_warnings)warns.add(new JsonPrimitive(s));
      res.add("warnings", warns);
      if(_key != null)
        res.addProperty("model_key",_key.toString());
      JsonArray vals = new JsonArray();
      if(_vals != null) for(GLMValidation val:_vals)
        vals.add(val.toJson());
      res.add("validations",vals);
      return res;
    }
  }

  public abstract GLMModel solve(GLMModel model, ValueArray ary);

  static final class Gram extends Iced {
    double [][] _xx;
    double   [] _xy;
    double      _yy;
    long        _nobs;
    final int   _N;
    public Gram(int N, boolean computeXX){
      _N = N;
      _xy = MemoryManager.malloc8d(N);
      if (computeXX) {
        _xx = new double[N][];
        for(int i = 0; i < N; ++i)
          _xx[i] = MemoryManager.malloc8d(i+1);
      }
    }
    public double [][] getXX(){
      double [][] xx = new double[_N][_N];
      for( int i = 0; i < _N; ++i ) {
        for( int j = 0; j < _xx[i].length; ++j ) {
            xx[i][j] = _xx[i][j];
            xx[j][i] = _xx[i][j];
        }
      }
      return xx;
    }
    public double [] getXY(){return _xy;}

    public void add(Gram grm){
      _yy += grm._yy;
      _nobs += grm._nobs;
      for(int i = 0; i < _N; ++i){
        _xy[i] += grm._xy[i];
        final int n = _xx[i].length;
        if(_xx != null)for(int j = 0; j < n; ++j)
          _xx[i][j] += grm._xx[i][j];
      }
    }
  }


  public static class GramMatrixFactory extends RowAlgFactory<Gram>{
    final int N;
    public GramMatrixFactory(int n){N = n;}
    public RowAlg<Gram> createInstance(){return new GramMatrixFunc(N, new Gram(N,true));}
  }

  /**
   * Task to compute gram matrix (X'*X).
   *
   * @author tomasnykodym
   *
   */
  public static class GramMatrixFunc extends RowAlg<Gram> {
    Gram _gram;
    final int N;

    public GramMatrixFunc(int n){N = n;}
    protected GramMatrixFunc(int n, Gram g){
      N = n;
      _gram = g;
    }

    @Override
    public GramMatrixFunc clone(){
      return new GramMatrixFunc(N,new Gram(N,true));
    }

    protected final void processRow(double[] x, int dense, int[] indexes, double y, double w){
      _gram._yy += 0.5*w*y*y;
      ++_gram._nobs;
      x[x.length-1] = 1.0; // intercept
      for(int i = 0; i < dense; ++i){
        for(int j = 0; j <= i; ++j)
          _gram._xx[i][j] += w*x[i]*x[j];
        _gram._xy[i] += w*y*x[i];
      }
      for(int i = 0; i < indexes.length; ++i){
        for(int j = 0; j < dense; ++j)
          _gram._xx[indexes[i]][j] += w*x[dense+i]*x[j];
        for(int j = 0; j <= i; ++j)
          _gram._xx[indexes[i]][indexes[j]] += w*x[dense+i]*x[dense+j];
        _gram._xy[indexes[i]] += w*y*x[dense+i];
      }
    }
    @Override
    public void processRow(double[] x, int dense, int[] indexes) {
      double y = x[dense-1];
      processRow(x, dense, indexes, y,1);
    }

    @Override
    public void reduce(RowAlg rwa) {
      GramMatrixFunc grm = (GramMatrixFunc)rwa;
      assert grm != this;
      if(_gram == null)
        _gram = grm._gram;
      else
        _gram.add(grm._gram);
    }

    @Override
    protected Gram res() {
      return _gram;
    }
  }

  private static double betaDiff(double [] b1, double [] b2){
    double res = Math.abs(b1[0] - b2[0]);
    for(int i = 1; i < b1.length; ++i)
      res = Math.max(res,Math.abs(b1[i] - b2[i]));
    return res;
  }

  public static GLMModel buildModel(DataFrame data, GLMParams glmp, LSMSolver solver, Sampling s){
    GramMatrixFactory gramF = new GramMatrixFactory(data.expandedVectorSize());
    double [] newBeta = new double [data.expandedVectorSize()];
    ArrayList<String> warns = new ArrayList<String>();
    boolean converged = false;
    Gram gram = gramF.apply(data,s);
    double [][] xx = gram.getXX();
    double nobsInv = 1.0/gram._nobs;
    double weight = glmp._family.weightApproximation();

    double [] xy = gram.getXY();
    // apply weight
    double w = weight*nobsInv;
    for(int i = 0; i < xx.length; ++i){
      xy[i] *= w;
      for(int j = 0; j < xx.length; ++j)
        xx[i][j] *= w;
    }
    double yy = gram._yy*w;
    solver.solve(xx, xy, yy, newBeta);
    if(glmp._family != Family.gaussian) { // IRLSM
      double [] beta = new double [newBeta.length];
      int iter = 0;
      do{
        double [] b = beta;
        beta = newBeta;
        newBeta = b;
        GLMZUpdateFactory zupdate = new GLMZUpdateFactory(glmp,beta);
        Gram zup = zupdate.apply(data);
        double [] z = zup._xy;
        for(int i = 0; i < z.length; ++i)
          z[i] *= nobsInv;
        solver.solve(xx, zup._xy, zup._yy * nobsInv, newBeta);
      } while(betaDiff(beta,newBeta) > glmp._betaEps && ++iter != 50);
      if(iter == 50)System.err.println("did not converge!, last betaDiff = " + betaDiff(beta, newBeta));
    }
    String [] warnings = new String[warns.size()];
    warns.toArray(warnings);
    if(data._standardize){
      // de-normalize coefficients
      double norm = 0;
      for(int i = 0; i < newBeta.length-1; ++i){
        newBeta[i] *= data.scale(i);
        norm += newBeta[i]*data.center(i);
      }
      newBeta[newBeta.length-1] -= norm;
    }
    return new GLMModel(data, newBeta, glmp, converged, warnings);
  }

  public static class ZUpdate extends Iced {
    public double [][] _XX;
    public double []   _Xz;
    public double      _zz;


    public void add(ZUpdate other){
      _zz += other._zz;
      for(int i = 0; i < _Xz.length; ++i){
        _Xz[i] += other._Xz[i];
        if(_XX != null) for(int j = 0; j < _XX[i].length;++j)
          _XX[i][j] += other._XX[i][j];
      }
    }
  }
  public static class GLMZUpdateFactory extends GramMatrixFactory{
    final GLMParams _glmp;
    final double [] _beta;

    public GLMZUpdateFactory(GLMParams glmp, double [] beta){
      super(beta.length);
      _glmp = glmp;
      _beta = beta;
    }

    @Override
    public RowAlg<Gram> createInstance() {
      return new GLMZUpdate(this);
    }

    private static class GLMZUpdate extends GramMatrixFunc {
      final transient GLMZUpdateFactory _factory;
      Gram _res;
      final double _xw;
      final boolean _reweightX;

      public GLMZUpdate(GLMZUpdateFactory f){
        super(f.N);
        _factory = f;
        _reweightX = f._glmp._reweightGram;
        _res = new Gram(_factory._beta.length,_reweightX);
        if(!_reweightX)_xw = Math.sqrt(_factory._glmp._family.weightApproximation());
        else _xw = 0;
      }


      @Override
      public void processRow(double[] x, int nDense, int[] indexes) {
        double mu = 0;
        double yr = x[nDense-1];
        if(_factory._glmp._caseMode != CaseMode.none)
          yr = _factory._glmp._caseMode.isCase(_factory._glmp._caseVal, yr)?1:0;
        x[nDense-1] = 1; // the intercept
        for(int i = 0; i < nDense; ++i)
          mu += _factory._beta[i]*x[i];
        for(int i = 0; i < indexes.length; ++i)
          mu += _factory._beta[indexes[i]]*x[nDense+i];
        double p = _factory._glmp._link.linkInv(mu);
        double var = _factory._glmp._family.variance(p);
        double z = mu + (yr-p)/var;
        if(_reweightX)
          super.processRow(x, nDense, indexes, z, var);
        else {
          double w = Math.sqrt(var)*_xw;
          _res._yy += 0.5*var*z*z;
          for(int i = 0; i < nDense; ++i)
            _res._xy[i] += w*z*x[i];
          for(int i = 0; i < indexes.length; ++i)
            _res._xy[indexes[i]] += w*z*x[nDense+i];
        }
      }
      @Override
      public void reduce(RowAlg rwa) {
        GLMZUpdate t = (GLMZUpdate)rwa;
        if(_res == null)_res = t._res;
        else _res.add(t._res);
      }
      @Override
      protected Gram res() {
        return _res;
      }
    }
  }

  public static class GLMValidation extends Iced {
    public static final String KEY_PREFIX = "__GLMValidation_";
    public final GLMParams _glmp;
    Key [] _modelKeys;
    Key _key;
    Key _modelKey;
    Sampling _s;
    public final long _n;
    public final double _dof;
    public final double _aic;
    public final double _deviance;
    public final double _nullDeviance;
    ErrMetric _errMetric = ErrMetric.SUMC;
    double _auc;
    public final ConfusionMatrix [] _cm;
    int _tid;
    double [] _thresholds;

    public GLMValidation(GLMParams glmp, double resDev, double nullDev, long n, long dof, ConfusionMatrix [] cm, double [] thresholds){
      _glmp = glmp;
      _deviance = resDev;
      _nullDeviance = nullDev;
      _n = n;
      _cm = cm;
      _dof = dof;
      _aic = glmp._family.aic(_deviance, _n, dof);
      _thresholds = thresholds;
      if(_cm != null){
        computeBestThreshold(ErrMetric.SUMC);
        computeAUC();
      }
    }

    public GLMValidation(GLMModel [] models, ErrMetric m, double [] thresholds) {
      _errMetric = m;
      _modelKeys = new Key[models.length];
      _glmp = models[0]._glmp;
      int i = 0;
      boolean solved = true;
      for(GLMModel xm:models){
        if(xm._key == null)xm.store();
        _modelKeys[i++] = xm._key;
        if(!xm.is_solved())solved = false;
      }
      if(!solved){
        _aic = Double.NaN;
        _dof = Double.NaN;
        _auc = Double.NaN;
        _deviance = Double.NaN;
        _nullDeviance = Double.NaN;
        _n = -1;
        _cm = null;
        return;
      }
      long n = 0;
      double nDev = 0;
      double dev = 0;
      if(models[0]._vals[0]._cm != null){
        int nthresholds = models[0]._vals[0]._cm.length;
        _cm = new ConfusionMatrix[nthresholds];
        for(int t = 0; t < nthresholds; ++t)
          _cm[t] = models[0]._vals[0]._cm[t];
        n += models[0]._vals[0]._n;
        dev = models[0]._vals[0]._deviance;
        nDev = models[0]._vals[0]._nullDeviance;
        for(i = 1; i < models.length; ++i){
          n += models[i]._vals[0]._n;
          dev += models[0]._vals[0]._deviance;
          nDev += models[0]._vals[0]._nullDeviance;
          for(int t = 0; t < nthresholds; ++t)
            _cm[t].add(models[i]._vals[0]._cm[t]);
        }
        _thresholds = thresholds;
        computeBestThreshold(m);
        computeAUC();
      } else {
        _cm = null;
        for(GLMModel xm:models){
          n += xm._vals[0]._n;
          dev += xm._vals[0]._deviance;
          nDev += xm._vals[0]._nullDeviance;
        }
      }
      _deviance = dev;
      _nullDeviance = nDev;
      _n = n;
      _aic = models[0]._glmp._family.aic(_deviance, _n, models[0]._beta.length);
      _dof = _n - models[0]._beta.length - 1;
    }

    public Iterable<GLMModel> models(){
      final Key [] keys = _modelKeys;
      final int N = (keys != null)?keys.length:0;
      return new Iterable<GLMModel> (){
        int idx;
        @Override
        public Iterator<GLMModel> iterator() {
          return new Iterator<GLMModel>() {
            @Override
            public void remove() {throw new UnsupportedOperationException();}
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

    public int fold(){
      return (_modelKeys == null)?1:_modelKeys.length;
    }

    public ConfusionMatrix bestCM(){
      if(_cm == null)return null;
      return bestCM(ErrMetric.SUMC);
    }

    public ConfusionMatrix bestCM(ErrMetric errM){
      computeBestThreshold(errM);
      return _cm[_tid];
    }

    public double bestThreshold() {
      return (_thresholds != null)?_thresholds[_tid]:0;
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
      if(_s != null)
        res.addProperty("sampling", _s.toString());
      res.addProperty("nrows", _n);
      res.addProperty("dof", _dof);
      res.addProperty("resDev", _deviance);
      res.addProperty("nullDev", _nullDeviance);
      res.addProperty("auc", _auc);

      if(_cm != null) {
        double [] err = _cm[_tid].classErr();
        JsonArray arr = new JsonArray();
        for(int i = 0; i < err.length; ++i)
          arr.add(new JsonPrimitive(err[i]));
        res.add("classErr", arr);
        res.addProperty("threshold", _thresholds[_tid]);
        res.add("cm", _cm[_tid].toJson());
      }
      if(_modelKeys != null){
        JsonArray arr = new JsonArray();
        for(Key k:_modelKeys)
          arr.add(new JsonPrimitive(k.toString()));
        res.add("xval_models", arr);
      }
      return res;
    }

    public double AIC() {
      return _aic;
    }
  }

  public static final class GLMValidationFactory extends RowAlgFactory<GLMValidation> {
    final GLMParams _glmp;
    final double    _ymu;
    final double [] _beta;
    final double [] _thresholds;

    public GLMValidationFactory(GLMParams glmp, double ymu, double [] beta, double [] thresholds){
      _glmp = glmp;
      _ymu = ymu;
      _beta = beta;
      _thresholds = thresholds;
    }

    @Override
    public RowAlg<GLMValidation> createInstance(){
      return new ValidationAlg(this);
    }

    private static class ValidationAlg extends RowAlg<GLMValidation> {
      final transient GLMValidationFactory _factory;
      long _n;
      long _caseCount;
      double _deviance;
      double _nullDeviance;
      double _err;
      ConfusionMatrix [] _cm;

      public ValidationAlg(GLMValidationFactory f){
        _factory = f;
        if(_factory._glmp._family == Family.binomial) {
          _cm = new ConfusionMatrix[_factory._thresholds.length];
          for(int i = 0; i < _factory._thresholds.length; ++i)
            _cm[i] = new ConfusionMatrix(2);
        }
      }

      @Override
      public void processRow(double[] x, int dense, int[] indexes) {
        ++_n;
        double yr = x[dense-1];
        x[dense-1] = 1.0;
        double ym = 0;
        for(int i = 0; i < dense; ++i)
          ym += _factory._beta[i] * x[i];
        for(int i = 0; i < indexes.length; ++i)
          ym += _factory._beta[dense+i] * x[indexes[i]];

        ym = _factory._glmp._link.linkInv(ym);
        if(_factory._glmp._caseMode != CaseMode.none)
          yr = _factory._glmp._caseMode.isCase(yr, _factory._glmp._caseVal)?1:0;
        if(yr == 1)
          ++_caseCount;
        _deviance += _factory._glmp._family.deviance(yr, ym);
        _nullDeviance += _factory._glmp._family.deviance(yr, _factory._ymu);
        if(_factory._glmp._family == Family.binomial) {
          if(yr < 0 || yr > 1 )
            throw new Error("response variable value out of range: " + yr);
          int i = 0;
          for(double t:_factory._thresholds){
            int p = ym >= t?1:0;
            _cm[i++].add((int)yr,p);
          }
        } else
          _err += (ym - yr)*(ym - yr);
      }

      @Override
      public void reduce(RowAlg ralg) {
        ValidationAlg other = (ValidationAlg)ralg;
        _n += other._n;
        _nullDeviance += other._nullDeviance;
        _deviance += other._deviance;
        _err += other._err;
        _caseCount += other._caseCount;
        if(_cm != null) {
          for(int i = 0; i < _factory._thresholds.length; ++i)
            _cm[i].add(other._cm[i]);
        } else
          _cm = other._cm;
      }

      @Override
      protected GLMValidation res() {
        if(_factory._glmp._family == Family.binomial){
          double p = _caseCount/(double)_n;
          _nullDeviance = -2*(_caseCount*Math.log(p) + (_n - _caseCount)*Math.log(1-p));
        }
        return new GLMValidation(_factory._glmp, _deviance, _nullDeviance, _n, _n - _factory._beta.length, _cm, _factory._thresholds);
      }
    }
  }
}
