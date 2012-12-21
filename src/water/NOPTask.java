package water;

public class NOPTask extends DTask {
  public NOPTask invoke(H2ONode h2o) { throw H2O.unimpl(); }
  public void compute() { throw H2O.unimpl(); }
}
