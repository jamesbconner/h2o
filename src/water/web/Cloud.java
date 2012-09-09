package water.web;

import java.util.Properties;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import water.H2O;
import water.H2ONode;
import water.HeartBeatThread;

public class Cloud extends H2OPage {

  public Cloud() {
    _refresh = 5;
  }

  @Override
  public JsonElement serverJson(Server server, Properties parms) {
    JsonObject res = new JsonObject();
    final H2O cloud = H2O.CLOUD;
    res.addProperty("cloud_name", H2O.NAME);
    res.addProperty("node_name", H2O.SELF.toString());
    res.addProperty("cloud_size",cloud._memary.length);
    return res;
  }

  @Override protected String serve_impl(Properties args) {
    RString response = new RString(html);
    response.replace("cloud_name",H2O.NAME);
    response.replace("node_name",H2O.SELF.toString());
    final H2O cloud = H2O.CLOUD;
    for( H2ONode h2o : cloud._memary ) {
      // restart the table line
      RString row = response.restartGroup("tableRow");
      // This hangs on ipv6 name resolution
      //String name = h2o._inet.getHostName();
      row.replace("host",h2o);
      row.replace("node",h2o);
      row.replace("num_cpus" ,            h2o.get_num_cpus () );
      row.replace("free_mem" ,toMegabytes(h2o.get_free_mem ()));
      row.replace("tot_mem"  ,toMegabytes(h2o.get_tot_mem  ()));
      row.replace("max_mem"  ,toMegabytes(h2o.get_max_mem  ()));
      row.replace("num_keys" ,           (h2o.get_keys     ()));
      row.replace("val_size" ,toMegabytes(h2o.get_valsz    ()));
      row.replace("free_disk",toMegabytes(h2o.get_free_disk()));
      row.replace("max_disk" ,toMegabytes(h2o.get_max_disk ()));

      row.replace("cpu_util" ,pos_neg(h2o.get_cpu_util()));

      double [] cpu_load = h2o.get_cpu_load();
      row.replace("cpu_load_1" ,pos_neg(cpu_load[0]));
      row.replace("cpu_load_5" ,pos_neg(cpu_load[1]));
      row.replace("cpu_load_15",pos_neg(cpu_load[2]));

      int fjq_hi = h2o.get_fjqueue_hi();
      int fjt_hi = h2o.get_fjthrds_hi();
      if(fjq_hi > HeartBeatThread.QUEUEDEPTH)
        row.replace("queueStyleHi","background-color:green;");
      row.replace("fjthrds_hi" , fjt_hi);
      row.replace("fjqueue_hi" , fjq_hi);
      int fjq_lo = h2o.get_fjqueue_lo();
      int fjt_lo = h2o.get_fjthrds_lo();
      if(fjq_lo > HeartBeatThread.QUEUEDEPTH)
        row.replace("queueStyleLo","background-color:green;");
      row.replace("fjthrds_lo" , fjt_lo);
      row.replace("fjqueue_lo" , fjq_lo);
      row.replace("rpcs" ,                h2o.get_rpcs());
      row.replace("tcps_active" ,         h2o.get_tcps_active());
      row.replace("node_type" ,           h2o.get_node_type());

      row.append();
    }
    response.replace("size",cloud._memary.length);
    return response.toString();
  }

  static String toMegabytes(long what) {
    return Long.toString(what>>20)+"M";
  }

  static String pos_neg( double d ) {
    return d>=0 ? Double.toString(d) : "n/a";
  }

  private final String html =
    "<div class='alert alert-success'>"
    + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
    + "</div>\n"
    + "<p>The Local Cloud has %size members\n"
    + "<table class='table table-striped table-bordered table-condensed'>\n"
    + "<thead class=''><tr>\n"
    +     "<th rowspan=\"2\">Local Nodes</th>\n"
    +     "<th rowspan=\"2\">CPUs</th>\n"
    +     "<th rowspan=\"2\">Local Keys</th>\n"
    +     "<th colspan=\"4\" style='text-align:center'>Memory</th>\n"
    +     "<th colspan=\"2\" style='text-align:center'>Disk</th>\n"
    +     "<th colspan=\"4\" style='text-align:center'>CPU Load</th>\n"
    +     "<th colspan=\"3\" style='text-align:center'>Threads / Tasks</th>\n"
    +     "<th rowspan=\"2\">TCPs Active</th>\n"
    +     "<th rowspan=\"2\">Type</th>\n"
    + "</tr>\n"
    + "<tr>\n"
    +     "<th>Cached</th>\n"  // memory
    +     "<th>Free</th>\n"
    +     "<th>Total</th>\n"
    +     "<th>Max</th>\n"
    +     "<th>Free</th>\n"    // disk
    +     "<th>Max</th>\n"
    +     "<th>Util</th>\n"    // CPU
    +     "<th>1min</th>\n"
    +     "<th>5min</th>\n"
    +     "<th>15min</th>\n"
    +     "<th>RPCs</th>\n"    // Threads
    +     "<th>HI</th>\n"
    +     "<th>Norm</th>\n"
    + "</tr></thead>\n"
    + "<tbody>\n"
    + "%tableRow{"
    + "  <tr>"
    + "    <td><a href='Remote?Node=%host'>%node</a></td>"
    + "    <td>%num_cpus</td>"
    + "    <td>%num_keys</td>"
    + "    <td>%val_size</td>"
    + "    <td>%free_mem</td>"
    + "    <td>%tot_mem</td>"
    + "    <td>%max_mem</td>"
    + "    <td>%free_disk</td>"
    + "    <td>%max_disk</td>"
    + "    <td>%cpu_util</td>"
    + "    <td>%cpu_load_1</td>"
    + "    <td>%cpu_load_5</td>"
    + "    <td>%cpu_load_15</td>"
    + "    <td>%rpcs</td>"
    + "    <td style='%queueStyleHi'>%fjthrds_hi / %fjqueue_hi</td>"
    + "    <td style='%queueStyleLo'>%fjthrds_lo / %fjqueue_lo</td>"
    + "    <td>%tcps_active</td>"
    + "    <td>%node_type</td>"
    + "  </tr>\n"
    + "}"
    + "</tbody>"
    + "</table>\n"
    ;
}
