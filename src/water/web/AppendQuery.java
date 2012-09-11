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
public class AppendQuery extends H2OPage {
  static final String html =
            "<p>You can update value with given text:</p>"
          + "<form class='well form-inline' action='Append'>"
          + "  <input type='text' class='input-small span3' placeholder='key' name='Key' id='Key' maxlength='512'>"
          + "  <input type='text' class='input-small span7' placeholder='text to append' name='Append' id='Append'>"
          + "  <button type='submit' class='btn btn-primary'>Append</button>"
          + "</form> "
          ;
  
  @Override protected String serveImpl(Server server, Properties args) {
    return html;        
          
    
  }
  
}
