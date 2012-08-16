/**
 * 
 */
package water.web;

import java.util.Properties;

import water.H2O;
import water.H2ONode;

/**
 * Network statistics web page.
 * 
 * The page contains network statistics for each running node (JVM).
 * 
 * @author Michal Malohlava
 * 
 */
public class Network extends H2OPage {

    public Network() {
        _refresh = 5;
    }

    @Override
    protected String serve_impl(Properties args) {
        RString response = new RString(HTML_TEMPLATE);
        
        response.replace("cloud_name", H2O.CLOUD.NAME);
        response.replace("node_name", H2O.SELF.toString());
        
        final H2O cloud = H2O.CLOUD;
        for (H2ONode h2o : cloud._memary) {
            RString row = response.restartGroup("tableRow");
            row.replace("node", h2o);
            // fill number of connections
            row.replace("total_in_conn", toPosNumber(h2o.get_total_in_conn()));
            row.replace("total_out_conn", toPosNumber(h2o.get_total_out_conn()));
            row.replace("tcp_in_conn", toPosNumber(h2o.get_tcp_in_conn()));
            row.replace("tcp_out_conn", toPosNumber(h2o.get_tcp_out_conn()));
            row.replace("udp_in_conn", toPosNumber(h2o.get_udp_in_conn()));
            row.replace("udp_out_conn", toPosNumber(h2o.get_udp_out_conn()));            

            // fill total traffic statistics
            row.replace("total_packets_recv", toPosNumber(h2o.get_total_packets_recv()));            
            row.replace("total_bytes_recv", toBytes(h2o.get_total_bytes_recv()));
            row.replace("total_bytes_recv_rate", toBytesPerSecond(h2o.get_total_bytes_recv_rate()));
            row.replace("total_packets_sent", toPosNumber(h2o.get_total_packets_sent()));
            row.replace("total_bytes_sent", toBytes(h2o.get_total_bytes_sent()));
            row.replace("total_bytes_sent_rate", toBytesPerSecond(h2o.get_total_bytes_sent_rate()));

            // fill TCP traffic statistics
            row.replace("tcp_packets_recv", toPosNumber(h2o.get_tcp_packets_recv()));
            row.replace("tcp_bytes_recv", toBytes(h2o.get_tcp_bytes_recv()));
            row.replace("tcp_packets_sent", toPosNumber(h2o.get_tcp_packets_sent()));
            row.replace("tcp_bytes_sent", toBytes(h2o.get_tcp_bytes_sent()));

            // fill UDP traffic statistics
            row.replace("udp_packets_recv", toPosNumber(h2o.get_udp_packets_recv()));
            row.replace("udp_bytes_recv", toBytes(h2o.get_udp_bytes_recv()));
            row.replace("udp_packets_sent", toPosNumber(h2o.get_udp_packets_sent()));
            row.replace("udp_bytes_sent", toBytes(h2o.get_udp_bytes_sent()));

            row.append();
        }

        return response.toString();
    }
    
    protected String toPosNumber(final long num) {
        if (num < 0) {
            return NOT_AVAILABLE;            
        } else {
            return String.valueOf(num);            
        }
    }

    protected String toBytes(final long num) {
        if (num < 0) {
            return NOT_AVAILABLE;            
        } else {
            return toCorrectUnit(num);            
        }
    }
    
    protected String toBytesPerSecond(final long num) {
        if (num < 0) {
            return NOT_AVAILABLE;            
        } else {
            return toCorrectUnit(num) + "/s";            
        }
    }
    
    protected String toCorrectUnit(final long num) {
        byte unit = 0; // 0 ~ Bytes
        long number = num;
        while (number > 1024 && unit < UNITS.length) {
            number = number >> 10;
            unit++;
        }  
        
        return number + " " + UNITS[unit];        
    }

    // Note: Open Sockets | TCP/UDP | state | recvs, packets, read/s write/s bytes/s | sends, packets, read/s, write/s, bytes/s

    private static final String HTML_TEMPLATE = "<div class='alert alert-success'>"
            + "You are connected to cloud <strong>%cloud_name</strong> and node <strong>%node_name</strong>."
            + "</div>"
            + "<table class='table table-striped table-bordered table-condensed'>"
            + "<thead class=''>"
            + "<tr><th>&nbsp;</th><th colspan='3'>Connections</th><th colspan='3'>Network interface traffic</th><th colspan='2'>TCP traffic</th><th colspan='2'>UDP traffic</th></tr>"
            + "<tr><th>Nodes</th><th>Total IN / OUT</th><th>TCP IN / OUT</th><th>UDP IN / OUT</th><th>Packets IN / OUT</th><th>Bytes IN / OUT</th><th>Rate IN / OUT</th><th>Packets IN / OUT</th><th>Bytes IN / OUT</th><th>Packets IN / OUT</th><th>Bytes IN / OUT</th></tr>"
            + "</thead>"
            + "<tbody>"
            + "%tableRow{"
            + "  <tr>"
            + "    <td>%node</td>"
            + "    <td>%total_in_conn / %total_out_conn</td>"
            + "    <td>%tcp_in_conn / %tcp_out_conn</td>"
            + "    <td>%udp_in_conn / %udp_out_conn</td>"
            + "    <td>%total_packets_recv / %total_packets_sent</td>"            
            + "    <td>%total_bytes_recv / %total_bytes_sent</td>"
            + "    <td>%total_bytes_recv_rate / %total_bytes_sent_rate</td>"
            + "    <td>%tcp_packets_recv / %tcp_packets_sent</td>"
            + "    <td>%tcp_bytes_recv / %tcp_bytes_sent</td>"
            + "    <td>%udp_packets_recv / %udp_packets_sent</td>"
            + "    <td>%udp_bytes_recv / %udp_bytes_sent</td>"
            + "  </tr>"
            + "}"
            + "</tbody>"
            + "</table>\n";
    
    private static final String NOT_AVAILABLE = "n/a";
    
    private static final String UNITS[] = {"B", "KB", "MB", "GB"};
    
}
