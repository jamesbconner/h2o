package water.web;

import java.util.Properties;

/** Stores a key using either GET or POST method
 *
 * @author peta
 */
public class PutQuery extends H2OPage {
  static final String html =
    "<p>You may either put a value:</p>"
    + "<form class='well form-inline' action='PutValue'>"
    + "  <input type='text' class='input-small span4' placeholder='value' name='Value' id='Value'>"
    + "  <input type='text' class='input-small span3' placeholder='key (optional)' name='Key' id='Key' maxlength='512'>"
    + "  <input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'>"
    + "  <button type='submit' class='btn btn-primary'>Put</button>"
    + "</form> "
    + "<p>or you may select a local file to be uploaded:"
    + "<form class='well form-inline' action='PutFile' enctype='multipart/form-data' method='post'>"
    + "  <input type='file' class='input-small span4' placeholder='value' name='File' id='File'>"
    + "  <input type='text' class='input-small span3' placeholder='key (optional)' name='Key' id='Key' maxlength='512'>"
    + "  <input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'>"
    + "  <button type='submit' class='btn btn-primary'>Put</button>"
    + "</form> "
    ;
  
  @Override protected String serve_impl(Properties args) {
    return html;
  }
}
