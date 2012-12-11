
package water.api;

/**
 *
 * @author peta
 */
public abstract class RequestResponse extends RequestArguments {

  protected abstract Response serve();



  public static class Response {
    public static enum State {
      done,
      timeout
      ;
    }

    public static Response createDone() {

    }

    public static Response createTimeout(Request timeoutRequest, Properties timeoutArguments) {
      
    }


  }

}
