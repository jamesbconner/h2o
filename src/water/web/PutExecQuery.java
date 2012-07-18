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
public class PutExecQuery extends H2OPage {
  static final String html =
            "<p>Select the jar file to upload, its main class and arguments. Upon upload the file will be compiled and executed:</p>"
          + "<form class='well form-inline' action='PutExec' enctype='multipart/form-data' method='post'>"
          + "  <input type='file' class='input-small span4' placeholder='jar file to upload' name='File' id='File'>"
          + "  <input type='text' class='input-small span3' placeholder='key (Class to be executed)' name='Key' id='Key' maxlength='512'>"
          + "  <input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF'>"
          + "  <button type='submit' class='btn btn-primary'>Put &amp; Execute</button><br/><br/>"
          + "  <input type='text' class='input-small span10' placeholder='arguments separated by &amp;' name='Args' id='Args'/>"
          + "</form> "
          ;
  
  @Override protected String serve_impl(Properties args) {
    return html;        
  }
  
}
