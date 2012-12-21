
package water.api;

import java.util.Arrays;

/**
 *
 * @author peta
 */
public class GLM extends Request {

  enum GlmFamily {
    gaussian,
    binomial,
    poisson
  }

  enum GlmNorm {
    NONE,
    L1,
    L2,
    ENET
  }

  public static final String JSON_GLM_Y = "y";
  public static final String JSON_GLM_X = "x";
  public static final String JSON_GLM_NEG_X = "neg_x";
  public static final String JSON_GLM_FAMILY = "family";
  public static final String JSON_GLM_NORM = "norm";
  public static final String JSON_GLM_LAMBDA = "lambda";
  public static final String JSON_GLM_LAMBDA_2 = "lambda_2";
  public static final String JSON_GLM_RHO = "rho";
  public static final String JSON_GLM_ALPHA = "alpha";
  public static final String JSON_GLM_MAX_ITER = "max_iter";
  public static final String JSON_GLM_BETA_EPS = "beta_eps";
  public static final String JSON_GLM_WEIGHT = "weight";
  public static final String JSON_GLM_THRESHOLD = "threshold";
  public static final String JSON_GLM_XVAL = "xval";

  protected final H2OHexKey _key = new H2OHexKey(JSON_KEY);
  protected final H2OHexKeyCol _y = new H2OHexKeyCol(_key, JSON_GLM_Y);
  protected final EnumArgument<GlmFamily> _family = new EnumArgument(JSON_GLM_FAMILY,GlmFamily.gaussian);
  protected final EnumArgument<GlmNorm> _norm = new EnumArgument(JSON_GLM_NORM,GlmNorm.NONE);







  @Override
  protected Response serve() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
