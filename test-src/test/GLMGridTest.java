package test;

import hex.GLMSolver;
import hex.LSMSolver;
import org.junit.Test;
import water.*;

// Test grid-search over GLM args
public class GLMGridTest extends TestUtil {

  private static GLMSolver.GLMModel compute_glm_score( ValueArray va, int[] cols, GLMSolver.GLMParams glmp, LSMSolver lsms, double thresh ) {
    // Binomial (logistic) GLM solver
    glmp._f = GLMSolver.Family.binomial;
    glmp._l = glmp._f.defaultLink; // logit
    glmp._familyArgs = glmp._f.defaultArgs; // no case/weight.  default 0.5 thresh
    glmp._familyArgs[GLMSolver.FAMILY_ARGS_DECISION_THRESHOLD] = thresh;
    glmp._betaEps = 0.000001;
    glmp._maxIter = 100;
    GLMSolver glms = new GLMSolver(lsms,glmp);

    StringBuilder sb = new StringBuilder("[cols ");
    for( int i=0; i<cols.length-1; i++ ) sb.append(cols[i]).append(" ");
    sb.append("] thresh=").append(thresh);
    System.out.print(sb.toString());

    // Solve it!
    GLMSolver.GLMModel m = glms.computeGLM(va, cols, null);
    if( m._warnings != null )
      for( String s : m._warnings )
        System.err.println(s);

    // Validate / compute results
    if( m.is_solved() ) {
      m.validateOn(va,null);
      double res = m._vals[0]._deviance;
      System.out.println(", res dev="+res+" err="+m._vals[0]._cm.err());
    }
    return m;
  }

  // An array from 0 to length with increasing int columns, 
  // skipping 'skip' and 'class_col'  Add class_col at the end.
  private static void cols( int[] cols, int class_col, int skip ) {
    int i=0, j=0;
    while( j<cols.length-1 ) {
      if( i != class_col && i != skip )
        cols[j++] = i;
      i++;
    }
    cols[j] = class_col;
  }

  // Minimize residual deviance of prostate
  @Test public void test_PROSTATE_CSV() {
    Key k1=null;
    try {
      // Load dataset
      //k1 = loadAndParseKey("h.hex","smalldata/logreg/prostate.csv");
      k1 = loadAndParseKey("h.hex","smalldata/hhp_107_01.data.gz");
      ValueArray va = ValueArray.value(DKV.get(k1));
      // Default normalization solver
      LSMSolver lsms = LSMSolver.makeSolver(); // Default normalization of NONE
      // Binomial (logistic) GLM solver
      GLMSolver.GLMParams glmp = new GLMSolver.GLMParams();

      // Initial columns: all, with the class moved to the end
      final int class_col = 1;
      int[] cols = new int[va._cols.length];
      cols(cols,class_col,-1);

      GLMSolver.GLMModel m = compute_glm_score(va,cols,glmp,lsms,0.5);

      // Try with 1 column removed
      cols = new int[va._cols.length-1];
      cols(cols,class_col,-1);
      for( int skip=0; skip<va._cols.length; skip++ ) {
        if( skip != class_col ) {
          cols(cols,class_col,skip);
          m = compute_glm_score(va,cols,glmp,lsms,0.5);
        }
      }          

      // Pick with 'IDX' removed
      cols(cols,class_col,0);
      compute_glm_score(va,cols,glmp,lsms,0.5);

      // Schmoo over threshold
      for( double t = 0.0; t<=1.0; t += 0.1 )
        compute_glm_score(va,cols,glmp,lsms,t);
        
    } finally {
      UKV.remove(k1);
    }
  }
}
