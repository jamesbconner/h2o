/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.util.Properties;
import water.Key;

/**
 *
 * @author peta
 */
public class PutExec extends H2OPage {

  @Override protected String serve_impl(Properties args) {
    String x = "";
    Object res = PutFile.uploadFile(args);
    if (res instanceof String)
      return (String)res;
    x += success("File was successfully uploaded to key "+args.getProperty("Key"));
    res = Compile.compile(args.getProperty("Key"));
    if (res instanceof String)
      return x+(String)res;
    x += success("Key "+args.getProperty("Key")+" successfully compiled to key <strong>"+res.toString()+"</strong>.");
    res = Exec.execute((Key)res,args.getProperty("Args",""));
    return x+(String)res;
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key", "File" };
  }
  
}
