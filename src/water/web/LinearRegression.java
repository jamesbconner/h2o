package water.web;
import java.util.Properties;
import water.ValueArray;

public class LinearRegression extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args) {
    return ServletUtil.serveTwoParams(args, new ServletUtil.RunnableTask() {
      @Override
      public String run(ValueArray ary, int colA, int colB) {
	System.out.println("Running LR");
        return hexlytics.LinearRegression.run(ary,colA,colB);
      }
    });
  }
}
