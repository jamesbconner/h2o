package water.web;

import java.util.Properties;
import water.H2O;
import water.H2ONode;
import water.HeartBeatThread;

/**
 *
 * @author peta
 */
public class Cloud extends H2OPage {

  public Cloud() {
    _refresh = 5;
  }
  
  
  @Override protected String serve_impl(Properties args) {
    RString response = new RString(html);
    response.replace("cloud_name",H2O.CLOUD.NAME);
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
      row.replace("thr_cnt" , h2o.get_thread_count());

      row.replace("cpu_util" ,pos_neg(h2o.get_cpu_util()));

      double [] cpu_load = h2o.get_cpu_load();
      row.replace("cpu_load_1" ,pos_neg(cpu_load[0]));
      row.replace("cpu_load_5" ,pos_neg(cpu_load[1]));
      row.replace("cpu_load_15",pos_neg(cpu_load[2]));

      int fjq_depth = h2o.get_fjqueue_depth();
      if(fjq_depth > HeartBeatThread.QUEUEDEPTH) 
        row.replace("queueStyle","background-color:green;");
      row.replace("fjqueue_depth" , fjq_depth);
      row.replace("node_type" ,            h2o.get_node_type());

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
    + "</div>"
    + "<p>The Local Cloud has %size members"
    + "<table class='table table-striped table-bordered table-condensed'>"
    + "<thead class=''><th>Local Nodes<th>CPUs<th>Local Keys<th>Mem Cached<th>FreeMem<th>TotalMem<th>MaxMem<th>FreeDisk<th>MaxDisk<th>CPU Utilization<th>Threads<th>CPU Load (1min)<th>CPU Load (5min)<th>CPU Load (15min)<th>FJ Task Depth<th>Type</thead>"
    + "<tbody>"
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
    + "    <td>%thr_cnt</td>"
    + "    <td>%cpu_load_1</td>"
    + "    <td>%cpu_load_5</td>"
    + "    <td>%cpu_load_15</td>"
    + "    <td style='%queueStyle'>%fjqueue_depth</td>"
    + "    <td>%node_type</td>"
    + "  </tr>"
    + "}"
    + "</tbody>"
    + "</table>\n"
    ;
}
