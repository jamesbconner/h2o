package water.web;

import java.util.Properties;
import water.*;

/**
 *
 * @author cliffc
 */
public class Shutdown extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args) {
    UDPRebooted.global_kill();
    return "Shutting down";
  }
}
