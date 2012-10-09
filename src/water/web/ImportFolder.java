package water.web;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import water.*;

public class ImportFolder extends H2OPage {

  int importFile(File f, byte rf, int num, RString response) {
    if( f.isDirectory() ) {
      for( File f2 : f.listFiles() )
        num = importFile(f2,rf,num,response);
      return num;
    }

    RString row = response.restartGroup("entry");
    Key k = PersistNFS.decodeFile(f);
    String fname = f.getName();
    long size = f.length();
    Value val = (size < 2*ValueArray.chunk_size())
      ? new Value((int)size,0,k,Value.NFS)
      : new ValueArray(k,size,Value.NFS);
    val.setdsk();
    UKV.put(k,val);
    row.replace("contents",success("File "+fname+" imported as key <strong>"+k+"</strong>"));
    row.append();
    return num+1;
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    String folder = args.getProperty("Folder");
    int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    if ((rf<0) || (rf>127))
      return error("Replication factor must be from 0 to 127.");

    int num = 0;
    RString response = new RString(html);
    try {
      File root = new File(folder).getCanonicalFile();
      if( !root.exists() ) throw new IOException(root+" not found");
      num = importFile(root,(byte)rf,0,response);
    } catch (IOException e) {
      RString row = response.restartGroup("entry");
      row.replace("contents",error("Unable to import file <strong>"+folder+"</strong>:")+"<pre>"+e.toString()+"</pre>");
      row.append();
    }
    response.replace("num",num);
    return response.toString();
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Folder" };
  }

  private static final String html =
    "<p>Imported %num files in total:"
    + "%entry{ %contents }"
    ;
}
