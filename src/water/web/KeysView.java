package water.web;

import java.util.Arrays;
import java.util.Properties;
import water.*;

public class KeysView extends H2OPage {
  public static final int KEYS_PER_PAGE = 25;

  public KeysView() {
    _refresh = 5;
  }

  @Override protected String serveImpl(Server server, Properties args) {
    RString response = new RString(html);
    // get the offset index
    int offset = 0;
    try {
      offset = Integer.valueOf(args.getProperty("o", "0"));
    } catch( NumberFormatException e ) { /* pass */ }
    // write the response
    H2O cloud = H2O.CLOUD;         // Current eldest Cloud
    Object[] keys = H2O.keySet().toArray();
    int lastIndex = keys.length;
    // get only the prefixed ones
    String prefix = args.getProperty("Prefix","");
    if (!prefix.isEmpty()) {
      int i = 0;
      for (int j = 0; j< keys.length; ++j) {
        if (((Key)keys[j]).toString().startsWith(prefix)) {
          if (i!=j) {
            Object s = keys[i];
            keys[i] = keys[j];
            keys[j] = s;
          }
          ++i;
        }
      }
      lastIndex = i;
    }


    // get the code values first
    int i = 0;
    for (int j = 0; j<lastIndex; ++j) {
      if (((Key)keys[j]).user_allowed()) {
        if (i!=j) {
          Object s = keys[i];
          keys[i] = keys[j];
          keys[j] = s;
          ++i;
        }
      }
    }
    // sort the code values
    Arrays.sort(keys,0,i);
    int keysize = lastIndex;
    formatPagination(offset,keysize,prefix.isEmpty() ? prefix : "?Prefix="+prefix, response);
    offset *= KEYS_PER_PAGE;
    i = 0;
    for( Object o : keys ) {
      if (i>=lastIndex) break;
      Key key = (Key)o;
      // skip keys at the beginning
      if (offset>0) {
        --offset;
        continue;
      }
      Value val = H2O.get(key);
      if( val == null ) { // Racing delete nuked key?
        keysize--;              // Dont count these keys
        continue;
      }
      formatKeyRow(cloud,key,response);
      if( ++i >= KEYS_PER_PAGE ) break;     // Stop at some reasonable limit
    }
    response.replace("noOfKeys",keysize);
    response.replace("cloud_name",H2O.NAME);
    response.replace("node_name",H2O.SELF.toString());
    if (!prefix.isEmpty())
      response.replace("pvalue","value='"+prefix+"'");
    return response.toString();
  }
  private void formatPagination(int offset, int size, String prefix, RString response) {
    if (size<=KEYS_PER_PAGE)
      return;
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='pagination pagination-centered' style='margin:0px auto'><ul>");
    if (offset!=0) {
      sb.append("<li><a href='?o=0"+prefix+"'>First</li>");
      sb.append("<li><a href='?o="+(offset-1)+prefix+"'>&lt;&lt;</a></li>");
    }
    int j = 0;
    int i = offset - 5;
    while (j<10) {
      if (++i<0)
        continue;
      if (i>size/KEYS_PER_PAGE)
        break;
      if (i==offset)
        sb.append("<li class='active'><a href=''>"+i+"</li>");
      else
        sb.append("<li><a href='?o="+i+prefix+"'>"+i+"</li>");
      ++j;
    }
    if (offset < (size/KEYS_PER_PAGE)) {
      sb.append("<li><a href='?o="+(offset+1)+prefix+"'>&gt;&gt;</a></li>");
      sb.append("<li><a href='?o="+(size/KEYS_PER_PAGE)+prefix+"'>Last</a></li>");
    }
    sb.append("</ul></div>");
    String nav = sb.toString();
    response.replace("navup",nav);
  }

  private void formatKeyRow(H2O cloud, Key key, RString response) {
    RString row = response.restartGroup("tableRow");
    // Dump out the Key
    String ks = key.toString();
    row.replace("keyHref",key);
    row.replace("key",key.user_allowed() ? ks : "<code>"+key.toString()+"</code>");
    int d = key.count_disk_replicas();
    int r = key.desired();
    if( d < r )
      row.replace("replicationStyle","background-color:#ffc0c0;color:#ff0000;");
    row.replace("r1",d);
    row.replace("r2",r);
    row.replace("home",cloud._memary[key.home(cloud)]);
    // Dump out the 2nd replica
    int idx2 = cloud.D(key,1);
    if( idx2 != -1 )
      row.replace("home2",cloud._memary[idx2]);
    int repl = key.replica(cloud);
    row.replace("replica",(repl==255?"":("r"+repl)));
    // Now the first 100 bytes of Value as a String
    row.append();
  }

  final static String html =
            "<div class='alert alert-success'>"
          + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
          + "</div>"
          + "<form class='well form-inline' action='KeysView'>"
          + "  <input type='text' class='input-small span10' placeholder='filter' name='Prefix' id='Prefix' %pvalue maxlength='512'>"
          + "  <button type='submit' class='btn btn-primary'>Filter keys!</button>"
          + "</form>"
          + "<p>Displaying %noOfKeys keys"
          + "<p>%navup</p>"
          + "<table class='table table-striped table-bordered table-condensed'>"
          + "<colgroup><col/><col/><col style=\"text-align:center\"/><col/></colgroup>\n"
          + "<thead><th>Key<th>D/R<th>1st<th>2nd<th>replica#</thead>\n"
          + "<tbody>"
          + "%tableRow{"
          + "  <tr>"
          + "    <td><a href='/Get?Key=%keyHref'>%key</a></td>"
          + "    <td style='%replicationStyle'>%r1/%r2</td>"
          + "    <td>%home</td>"
          + "    <td>%home2</td>"
          + "    <td>%replica</td>"
          + "  </tr>\n"
          + "}"
          + "</tbody>"
          + "</table>\n"
          ;

}
