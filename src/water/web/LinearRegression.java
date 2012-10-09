package water.web;
import java.util.Properties;

import water.ValueArray;

public class LinearRegression extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    return ServletUtil.serveTwoParams(args, new ServletUtil.RunnableTask() {
      @Override
      public String run(ValueArray ary, int colA, int colB) {
        return hex.LinearRegression.run(ary,colA,colB);
      }
    });
  }
}
