package water.web;
import java.util.Properties;

import water.ValueArray;

/**
 * The servlet for launching a covariance computation
 * 
 * @author alex@0xdata.com
 */
public class Covariance extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    return ServletUtil.serveTwoParams(args, new ServletUtil.RunnableTask() {
      @Override
      public String run(ValueArray ary, int colA, int colB) {
        return hexlytics.Covariance.run(ary,colA,colB);
      }
    });
  }
}
