package water.web;
import java.util.Properties;
import water.ValueArray;

public class LinearRegression extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    return ServletUtil.serveTwoParams(args, new ServletUtil.RunnableTask() {
      @Override
      public String run(ValueArray ary, int colA, int colB) {
        return hexlytics.LinearRegression.run(ary,colA,colB);
      }
    });
  }
}
