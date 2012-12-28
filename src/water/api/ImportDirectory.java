package water.api;

import com.google.gson.*;

import water.Futures;
import water.Key;
import water.util.FileIntegrityChecker;

public class ImportDirectory extends Request {
  protected final ExistingDir _dir = new ExistingDir(FILE);

  public ImportDirectory() {
    _requestHelp = "Imports the given directory recursively.  All nodes in the cloud must have" +
        " an identical copy of the file structure in their local file systems.";
    _dir._requestHelp = "Dir to import from.";
  }

  @Override
  protected Response serve() {
    FileIntegrityChecker c = FileIntegrityChecker.check(_dir.value());

    JsonObject json = new JsonObject();

    JsonArray succ = new JsonArray();
    JsonArray fail = new JsonArray();
    Futures fs = new Futures();
    for( int i = 0; i < c.size(); ++i ) {
      Key k = c.importFile(i, fs);
      if( k == null ) {
        fail.add(new JsonPrimitive(c.getFileName(i)));
      } else {
        JsonObject o = new JsonObject();
        o.addProperty(KEY, k.toString());
        o.addProperty(FILE, c.getFileName(i));
        succ.add(o);
      }
    }
    fs.block_pending();

    json.add(SUCCEEDED, succ);
    json.add(FAILED, fail);

    Response r = Response.done(json);
    r.setBuilder(SUCCEEDED, new ArrayBuilder() {
      @Override
      public String header(JsonArray array) {
        return "<table class='table table-striped table-bordered'>" +
            "<tr><th>File</th></tr>";
      }

      @Override
      public Builder defaultBuilder(JsonElement element) {
        return new ObjectBuilder() {
          @Override
          public String build(Response response, JsonObject object,
              String contextName) {
            return "<tr><td>" +
                "<a href='Inspect.html?key="+object.get(KEY).getAsString()+"'>" +
                object.get(FILE).getAsString() +
                "</a></td></tr>";
          }
        };
      }
    });
    return r;
  }
}
