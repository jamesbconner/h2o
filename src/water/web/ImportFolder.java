package water.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import water.*;

public class ImportFolder extends H2OPage {


  public static class FolderIntegrityChecker extends DRemoteTask {
    String[] _files;
    long[] _sizes;
    short[] _ok;
    int _stringSizes;

    @Override public void compute() {
      _ok = new short[_files.length];
      for (int i = 0; i < _files.length; ++i) {
        File f = new File(_files[i]);
        if (f.exists() && (f.length()==_sizes[i]))
          _ok[i] = 1;
      }
      tryComplete();
    }

    @Override public void reduce(DRemoteTask drt) {
      FolderIntegrityChecker other = (FolderIntegrityChecker) drt;
      if( _ok == null ) _ok = other._ok;
      else for ( int i = 0; i < _ok.length; ++i ) _ok[i] += other._ok[i];
    }

    private void addFolder(File folder, ArrayList<File> filesInProgress ) {
      if (folder.isDirectory()) {
        for (File f: folder.listFiles()) {
          if (f.isDirectory())
            addFolder(f,filesInProgress);
          else
            filesInProgress.add(f);
        }
      } else {
        filesInProgress.add(folder);
      }
    }

    public FolderIntegrityChecker() {  }
    public FolderIntegrityChecker(File root) {
      ArrayList<File> filesInProgress = new ArrayList();
      addFolder(root,filesInProgress);
      _files = new String[filesInProgress.size()];
      _sizes = new long[filesInProgress.size()];
      _stringSizes = 0;
      for (int i = 0; i < _files.length; ++i) {
        File f = filesInProgress.get(i);
        _files[i] = f.getAbsolutePath();
        _sizes[i] = f.length();
        _stringSizes += _files[i].length();
      }
    }

    private Key importFile(int i) {
      if( _ok[i] < H2O.CLOUD.size() ) return null;
      File f = new File(_files[i]);
      Key k = PersistNFS.decodeFile(f);
      long size = f.length();
      Value val = (size < 2*ValueArray.CHUNK_SZ)
        ? new Value(k,(int)size,Value.NFS)
        : new ValueArray(k,size,Value.NFS).value();
      val.setdsk();
      UKV.put(k,val);
      return k;
    }

    public String importFilesHTML() {
      StringBuilder sb = new StringBuilder();
      int correct = 0;
      for (int i = 0; i < _files.length; ++i) {
        Key k = importFile(i);
        if (k == null) {
          sb.append(error("File <strong>"+_files[i]+"</strong> does not have the same size on all nodes."));
        } else {
          RString html = new RString("File <strong>%File</strong> imported as" +
           " key <a href='/Inspect?Key=%$Key'>%Key</a>");
          html.replace("File", _files[i]);
          html.replace("Key", k);
          sb.append(success(html.toString()));
          ++correct;
        }
      }
      return "Out of "+_files.length+" a total of "+correct+" was successfully imported to the cloud."+sb.toString();
    }

    public JsonObject importFilesJson() {
      JsonObject result = new JsonObject();
      JsonArray ok = new JsonArray();
      JsonArray failed = new JsonArray();
      int correct = 0;
      for (int i = 0; i < _files.length; ++i) {
        Key k = importFile(i);
        if (k == null) {
          failed.add(new JsonPrimitive(_files[i]));
        } else {
          ok.add(new JsonPrimitive(_files[i]));
          ++correct;
        }
      }
      result.add("failed",failed);
      result.add("ok",ok);
      result.addProperty("imported",correct);
      return result;
    }
  }


  FolderIntegrityChecker importFolder(Properties args) throws Exception {
    String folder = args.getProperty("Folder");
     File root = new File(folder);
    if (!root.exists())
      throw new Exception("Unable to import folder "+folder+". Folder not found.");
    folder = root.getCanonicalPath();
    FolderIntegrityChecker checker = new FolderIntegrityChecker(new File(folder));
    checker.invokeOnAllNodes();
    return checker;
  }


  @Override public JsonObject serverJson(Server server, Properties args, String sessionID) {
    try {
      return importFolder(args).importFilesJson();
    } catch (Exception e) {
      e.printStackTrace();
      JsonObject result = new JsonObject();
      result.addProperty("Error",e.getMessage());
      return result;
    }
  }


  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    try {
      return importFolder(args).importFilesHTML();
    } catch( Exception ex ) {
      ex.printStackTrace();
      return error(ex.getMessage());
    }
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Folder" };
  }
}
