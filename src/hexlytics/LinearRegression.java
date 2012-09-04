package hexlytics;
import java.io.*;
import water.*;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

/**
 * @author cliffc@0xdata.com
 */
public abstract class LinearRegression {

  static public String run( ValueArray ary, int colA, int colB ) {
    StringBuilder sb = new StringBuilder();
    
    sb.append("Linear Regression of ").append(ary._key).append(" between ").
      append(colA).append(" and ").append(colB);

    LR_Task lr = new LR_Task();
    lr._arykey = ary._key;
    lr._colA = colA;
    lr._colB = colB;

    // Pass 1: compute sums & sums-of-squares
    lr._pass = 1;
    long start = System.currentTimeMillis();
    lr.invoke(ary._key);
    long pass1 = System.currentTimeMillis();
    sb.append("<p>Pass 1 in ").append(pass1-start).append("msec");

    // Pass 2: Compute squared errors
    long n = lr._rows;
    lr._pass = 2;
    lr._Xbar = lr._sumX / n;
    lr._Ybar = lr._sumY / n;
    lr.reinitialize();
    lr.invoke(ary._key);
    long pass2 = System.currentTimeMillis();
    sb.append("<p>Pass 2 in ").append(pass2-pass1).append("msec");

    // Compute the regression
    lr._beta1 = lr._XYbar / lr._XXbar;
    lr._beta0 = lr._Ybar - lr._beta1 * lr._Xbar;
    // print results
    sb.append("<p>y = ").append(lr._beta1).append(" * x + ").append(lr._beta0);

    // Pass 3: analyze results
    lr._pass = 3;
    lr.reinitialize();
    lr.invoke(ary._key);
    long pass3 = System.currentTimeMillis();
    sb.append("<p>Pass 3 in ").append(pass3-pass2).append("msec");

    long df = n - 2;
    double R2    = lr._ssr / lr._YYbar;
    double svar  = lr._rss / df;
    double svar1 = svar / lr._XXbar;
    double svar0 = svar/n + lr._Xbar*lr._Xbar*svar1;
    sb.append("<p>R^2                 = ").append(R2);
    sb.append("<p>std error of beta_1 = ").append(Math.sqrt(svar1));
    sb.append("<p>std error of beta_0 = ").append(Math.sqrt(svar0));
    svar0 = svar * lr._sumX2 / (n * lr._XXbar);
    sb.append("<p>std error of beta_0 = ").append(Math.sqrt(svar0));
    
    sb.append("<p>SSTO = ").append(lr._YYbar);
    sb.append("<p>SSE  = ").append(lr._rss);
    sb.append("<p>SSR  = ").append(lr._ssr);

    return sb.toString();
  }

  @RTSerializer(LR_Task.Serializer.class)
  public static class LR_Task extends MRTask {
    Key _arykey;                // Main ValueArray key
    int _pass;                  // Pass 1, 2 or 3.
    int _colA, _colB;           // Which columns to work on
    long _rows;                 // Rows used
    double _sumX,_sumY,_sumX2;
    double _Xbar, _Ybar, _XXbar, _YYbar, _XYbar;
    double _beta0, _beta1;
    double _rss, _ssr;
    public static class Serializer extends RemoteTaskSerializer<LR_Task> {
      @Override public int wire_len(LR_Task t) { return 3*4+13*8+t._arykey.wire_len(); }
      @Override public int write( LR_Task t, byte[] buf, int off ) {
        off += UDP.set4 (buf,off,t._pass);
        off += UDP.set4 (buf,off,t._colA);
        off += UDP.set4 (buf,off,t._colB);
        off += UDP.set8 (buf,off,t._rows);
        off += UDP.set8d(buf,off,t._sumX);
        off += UDP.set8d(buf,off,t._sumY);
        off += UDP.set8d(buf,off,t._sumX2);
        off += UDP.set8d(buf,off,t._Xbar);
        off += UDP.set8d(buf,off,t._Ybar);
        off += UDP.set8d(buf,off,t._XXbar);
        off += UDP.set8d(buf,off,t._YYbar);
        off += UDP.set8d(buf,off,t._XYbar);
        off += UDP.set8d(buf,off,t._beta0);
        off += UDP.set8d(buf,off,t._beta1);
        off += UDP.set8d(buf,off,t._rss  );
        off += UDP.set8d(buf,off,t._ssr  );
        off += t._arykey.write(buf,off);
        return off;
      }
      @Override public LR_Task read( byte[] buf, int off ) { 
        LR_Task t = new LR_Task();
        t._pass = UDP.get4 (buf,(off+=4)-4);
        t._colA = UDP.get4 (buf,(off+=4)-4);
        t._colB = UDP.get4 (buf,(off+=4)-4);
        t._rows = UDP.get8 (buf,(off+=8)-8);
        t._sumX = UDP.get8d(buf,(off+=8)-8);
        t._sumY = UDP.get8d(buf,(off+=8)-8);
        t._sumX2= UDP.get8d(buf,(off+=8)-8);
        t._Xbar = UDP.get8d(buf,(off+=8)-8);
        t._Ybar = UDP.get8d(buf,(off+=8)-8);
        t._XXbar= UDP.get8d(buf,(off+=8)-8);
        t._YYbar= UDP.get8d(buf,(off+=8)-8);
        t._XYbar= UDP.get8d(buf,(off+=8)-8);
        t._beta0= UDP.get8d(buf,(off+=8)-8);
        t._beta1= UDP.get8d(buf,(off+=8)-8);
        t._rss  = UDP.get8d(buf,(off+=8)-8);
        t._ssr  = UDP.get8d(buf,(off+=8)-8);
        t._arykey  = Key.read(buf,off);
        return t;
      }
      @Override public void write( LR_Task t, DataOutputStream dos ) { throw new Error("do not call"); }
      @Override public LR_Task read ( DataInputStream  dis ) { throw new Error("do not call"); }
    }

    public void map( Key key ) {
      assert key.home();
      // Get the root ValueArray for the metadata
      ValueArray ary = (ValueArray)DKV.get(_arykey);
      // Get the raw bits to work on
      byte[] bits = DKV.get(key).get();
      // Split out all the loop-invariant meta-data offset into
      int rowsize = ary.row_size();
      int rows = bits.length/rowsize;
      int colA_off  = ary.col_off  (_colA);
      int colB_off  = ary.col_off  (_colB);
      int colA_size = ary.col_size (_colA);
      int colB_size = ary.col_size (_colB);
      int colA_base = ary.col_base (_colA);
      int colB_base = ary.col_base (_colB);
      int colA_scale= ary.col_scale(_colA);
      int colB_scale= ary.col_scale(_colB);
      boolean good = (ary.col_badat(_colA) == 0 && ary.col_badat(_colB) == 0);
      int bad = 0;
      // Loop over the data
      switch( _pass ) {
      case 1:                   // Pass 1
        // Run pass 1
        // double sumx = 0.0, sumy = 0.0, sumx2 = 0.0;
        // for( int i=0; i<n; i++ ) {
        //   sumx  += x[i];
        //   sumx2 += x[i] * x[i];
        //   sumy  += y[i];
        // }
        for( int i=0; i<rows; i++ ) {
          // Doing loop-unswitching here will move all this expensive code out
          // in the case where all the rows are known-good at the start.
          if( !good &&          // Guard more expensive tests
              (!ary.valid(bits,i,rowsize,colA_off,colA_size) ||
               !ary.valid(bits,i,rowsize,colB_off,colB_size)) ) {
            bad++;
            continue;
          }
          double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
          double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
          _sumX += X;
          _sumY += Y;
          _sumX2+= X*X;
        }
        _rows = rows-bad;
        break;

      case 2:                   // Pass 2
        // Run pass 2
        // second pass: compute summary statistics
        // double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
        // for (int i = 0; i < x.length; i++) {
        //   xxbar += (x[i] - xbar) * (x[i] - xbar);
        //   yybar += (y[i] - ybar) * (y[i] - ybar);
        //   xybar += (x[i] - xbar) * (y[i] - ybar);
        // }
        for( int i=0; i<rows; i++ ) {
          if( !good &&
              (!ary.valid(bits,i,rowsize,colA_off,colA_size) ||
               !ary.valid(bits,i,rowsize,colB_off,colB_size)) )
            continue;
          double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
          double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
          double Xa = (X-_Xbar);
          double Ya = (Y-_Ybar);
          _XXbar += Xa*Xa;
          _YYbar += Ya*Ya;
          _XYbar += Xa*Ya;
        }
        break;
      case 3:                   // Pass 3
        // Run pass 3
        //double rss = 0.0;      // residual sum of squares
        //double ssr = 0.0;      // regression sum of squares
        //for (int i = 0; i < n; i++) {
        //  double fit = beta1*x[i] + beta0;
        //  rss += (fit - y[i]) * (fit - y[i]);
        //  ssr += (fit - ybar) * (fit - ybar);
        //}
        for( int i=0; i<rows; i++ ) {
          if( !good &&
              (!ary.valid(bits,i,rowsize,colA_off,colA_size) ||
               !ary.valid(bits,i,rowsize,colB_off,colB_size)) )
            continue;
          double X = ary.datad(bits,i,rowsize,colA_off,colA_size,colA_base,colA_scale,_colA);
          double Y = ary.datad(bits,i,rowsize,colB_off,colB_size,colB_base,colB_scale,_colB);
          double fit = _beta1*X + _beta0;
          _rss += (fit -  Y   )*(fit - Y    );
          _ssr += (fit - _Ybar)*(fit - _Ybar);
        }
        break;
      }
    }

    public void reduce( DRemoteTask rt ) {
      LR_Task  lr = (LR_Task)rt;
      switch( _pass ) {
      case 1:
        _sumX += lr._sumX ;
        _sumY += lr._sumY ;
        _sumX2+= lr._sumX2;
        _rows += lr._rows ;
        break;
      case 2:
        _XXbar += lr._XXbar;
        _YYbar += lr._YYbar;
        _XYbar += lr._XYbar;
        break;
      case 3:
        _rss += lr._rss;
        _ssr += lr._ssr;
        break;
      }
    }
  }
}

