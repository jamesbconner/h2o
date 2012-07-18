/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;

/**
 *
 * @author peta
 */
public abstract class Page {

  public String[] requiredArguments() {
    return null;
  }
  
  public abstract Object serve(Server server, Properties args);
}
