package water.api;

import com.google.gson.JsonObject;

import water.Key;
import water.util.FileIntegrityChecker;

public class ImportFile extends Request {
  protected final ExistingFile _file = new ExistingFile(FILE);

  public ImportFile() {
    _requestHelp = "Imports the given file.  All nodes in the cloud must have" +
        " an identical copy of the file in their local file systems.";
    _file._requestHelp = "File to import.";
  }

  @Override
  protected Response serve() {
    FileIntegrityChecker c = FileIntegrityChecker.check(_file.value());
    Key k = c.importFile(0, null);
    if( k == null )
      return Response.error("Unable to import file: " + c.getFileName(0));

    JsonObject json = new JsonObject();
    json.addProperty(FILE, c.getFileName(0));
    json.addProperty(KEY, k.toString());

    Response r = Response.done(json);
    r.setBuilder(KEY, new KeyElementBuilder());
    return r;
  }
}
