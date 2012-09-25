package water.web;
import java.util.Properties;

import water.ValueArray;

/**
 * The servlet for launching a covariance computation
 *
 * @author alex@0xdata.com
 */
public class Covariance extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args) throws PageError {
    return ServletUtil.serveTwoParams(args, new ServletUtil.RunnableTask() {
      @Override
      public String run(ValueArray ary, int colA, int colB) {
        return hex.Covariance.run(ary,colA,colB);
      }
    });
  }
}
