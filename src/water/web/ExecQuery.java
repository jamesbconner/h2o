/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Arrays;
import java.util.Properties;
import water.H2O;
import water.Key;
import water.Value;
import water.ValueCode;

/**
 *
 * @author peta
 */
public class ExecQuery extends H2OPage {
  
  static final String html =
            "<script src='bootstrap/js/bootstrap-typeahead.js'></script>"
          + "<p>Plese write the name of a key you want to execute:</p>"
          + "<form class='well form-inline' action='Exec'>"
          + "  <input type='text' class='input-small span3' placeholder='key' name='Key' id='Key' maxlength='512' %kval data-provide='typeahead' data-items='8' data-source='%keys'/>&nbsp;"
          + "  <button type='submit' class='btn btn-primary'>Execute</button><br/><br/>"
          + "  <input type='text' class='input-small span8' placeholder='arguments separated by &amp;' name='Args' id='Args'/>"
          + "</form> "
          ;
  
//  static final RString response = new RString(html);

  @Override
  protected String serve_impl(Properties args) {
    String key = args.getProperty("Key","");
    RString response = new RString(html);
    response.replace("keys",getExecutableKeys());
    if (!key.isEmpty()) {
      key = "value='"+key+"'";
      response.replace("kval",key);
    }
    return response.toString();
  }

  /** Returns the executable keys 
   * 
   */
  
  String getExecutableKeys() {
    Object[] keys = H2O.keySet().toArray();
    int nextExecutable = 0;
    for (Object o : keys) {
      Key k = (Key)o;
      Value v = H2O.get(k);
      if (v == null)
        continue;
      if (!(v instanceof ValueCode))
        continue;
      keys[nextExecutable++] = k;
    }
    Arrays.sort(keys,0,nextExecutable);
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < nextExecutable; ++i) {
      if (i!=0)
        sb.append(",");
      sb.append("\"");
      sb.append(keys[i].toString());
      sb.append("\"");
    }
    sb.append("]");
    return sb.toString();
  }
  
  
}
