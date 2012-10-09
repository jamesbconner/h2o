package water.web;
import java.util.Properties;

import water.ValueArray;

public class LogisticRegression extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    return ServletUtil.serveTwoParams(args, new ServletUtil.RunnableTask() {
      @Override
      public String run(ValueArray ary, int colA, int colB) {
        return hex.LogisticRegression.web_main(ary._key, new int[]{colA}, colB).toString();
      }
    });
  }
}
