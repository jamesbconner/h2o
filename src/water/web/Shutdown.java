package water.web;

import java.util.Properties;
import water.*;

/**
 *
 * @author cliffc
 */
public class Shutdown extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    UDPRebooted.global_kill();
    return "Shutting down";
  }
}
