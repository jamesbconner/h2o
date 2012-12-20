
package water.api;

import java.io.File;

import com.google.gson.*;

public class WWWFiles extends Request {
  private final Str _filter = new Str(JSON_FILTER,"");
  private final Int _limit = new Int(JSON_LIMIT,1024,0,10240);

  public WWWFiles() {
    _requestHelp = "Provides a simple JSON array of filtered local files.";
    _filter._requestHelp = "Only files whose names contain the given filter " +
    		"will be returned.";
    _limit._requestHelp = "Max number of file names to be returned.";
  }

  @Override protected Response serve() {
    String f = _filter.value();
    int limit = _limit.value();

    File base = null;
    String filterPrefix = "";
    if( !f.isEmpty() ) {
      File file = new File(f);
      if( file.isDirectory() ) {
        base = file;
      } else {
        base = file.getParentFile();
        filterPrefix = file.getName().toLowerCase();
      }
    }
    if( base == null ) base = new File(".");

    JsonArray array = new JsonArray();
    for( File file : base.listFiles() ) {
      if( file.isHidden() ) continue;
      if( file.getName().toLowerCase().startsWith(filterPrefix) ) {
        String s = file.getPath();
        array.add(new JsonPrimitive(s));
      }
      if( array.size() == limit) break;
    }

    JsonObject response = new JsonObject();
    response.add(JSON_FILES, array);
    return Response.done(response);
  }
}
