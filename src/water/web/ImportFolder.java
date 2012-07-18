/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import water.Key;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class ImportFolder extends H2OPage {
  
  static int imported = 0;
  
  
  void importFile(File f, String name, byte rf, RString response) {
    RString row = response.restartGroup("entry");
    try {
      FileInputStream fis = new FileInputStream(f);
      Object res = ValueArray.read_put_file(name, fis , rf);
      row.replace("contents",success("File "+f.getAbsolutePath()+" imported as key <strong>"+name+"</strong>"));      
      imported += 1;
    } catch (IOException e) {
      row.replace("contents",error("Unable to import file <strong>"+f.getAbsolutePath()+"</strong>:")+"<pre>"+e.toString()+"</pre>");
    }          
    row.append();
    
  }
  
  void importFilesFromFolder(File folder, String prefix, byte rf, RString response) {
    
    if (!folder.exists()) {
      RString row = response.restartGroup("entry");
      row.replace("contents", error("Unable to import files from folder <strong>"+folder.getAbsolutePath()+"</strong>. Path not found."));
      row.append();
      return;
    }
    for (File f: folder.listFiles()) {
      String name = prefix+File.separator+f.getName();
      if (f.isDirectory()) {
        importFilesFromFolder(f,name,rf,response);
      } else {
        importFile(f,name,rf,response);
      }
    }
  }
  
  
  @Override protected String serve_impl(Properties args) {
    String folder = args.getProperty("Folder");
    String prefix = args.getProperty("Prefix",folder);
    if (prefix.isEmpty())
      prefix = folder;
    int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if ((rf<0) || (rf>127))
      return error("Replication factor must be from 0 to 127.");
    boolean recursive = args.getProperty("R","off").equals("on");
    
    RString response = new RString(html);
//    response.clear();
    imported = 0;

    File root = new File(folder);   
    if (root.isDirectory())
      importFilesFromFolder(root,prefix,(byte)rf,response);
    else
      importFile(root,prefix+File.separator+root.getName(),(byte)rf,response);
    
    response.replace("num",imported);
    return response.toString();
  }
  
  @Override public String[] requiredArguments() {
    return new String[] { "Folder" };
  }
  
  private static final String html =
            "<p>Imported %num files in total:"
          + "%entry{ %contents }"
          ;
  
//  private static final RString response = new RString(html);
  
}
