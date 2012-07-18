package water.web;

import java.util.Properties;

/**
 *
 * @author peta
 */
public class ImportQuery extends H2OPage {

  private static final String html =
            "<p>You may also specify a folder whose files should be imported as "
          + "keys to H2O. Please note that the folder must be local to the node "
          + "<p>You may specify the default replication factor for all the keys "
          + "that will be imported and you may also select their prefix. If no "
          + "prefix is specified then the folder path will be used as prefix. "
          + "<p>Please note that the folder path should be absolute, or you are "
          + "risking that the H2O will not be able to find it properly."
          + "<form class='well form-inline' action='ImportFolder'>"
          + "  <input type='text' class='input-small span8' placeholder='folder' name='Folder' id='Folder'>"
          + "  <button type='submit' class='btn btn-primary'>Import Folder</button><br /><br/>"
          + "  <input type='text' class='input-small span3' placeholder='key prefix' name='Prefix' id='Prefix' maxlength='512'>"      
          + "  <input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'>"
          + "  <input style='display:none' type='checkbox' class='input-small offset8' name='R' id='R'> import files recursively </input>"
          + "</form> "
          + "<p>Alternatively you can specify a URL to import from provided that "
          + "the node you are connected to can reach it:"
          + "<form class='well form-inline' action='ImportUrl'>"
          + "  <input type='url' class='input-small span4' placeholder='url' name='Url' id='Url'>"
          + "  <input type='text' class='input-small span3' placeholder='key (optional)' name='Key' id='Key' maxlength='512'>"
          + "  <input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'>"
          + "  <button type='submit' class='btn btn-primary'>Import URL</button>"
          + "</form> "
          ;
  
  
  @Override protected String serve_impl(Properties args) {
    return html;
  }
}
