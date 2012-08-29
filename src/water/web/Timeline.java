package water.web;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import water.H2O;
import water.H2ONode;
import water.TimeLine;
import water.UDP;

/**
 *
 * @author peta
 */
public class Timeline extends H2OPage {

  @Override protected String serve_impl(Properties args) {
    long ctm = System.currentTimeMillis();

    // Take a system-wide snapshot
    long[][] snapshot = TimeLine.system_snapshot();
    TimelineSnapshot events = new TimelineSnapshot(TimeLine.CLOUD, snapshot);
    H2O cloud = TimeLine.CLOUD;
    RString response = new RString(html);
//    response.clear();

    // Get the most recent event time
    response.replace("now",new SimpleDateFormat("yyyy.MM.dd HH:mm").format(new Date(ctm)));
    response.replace("self",H2O.SELF);

    // some pretty ways to view time
    SimpleDateFormat sdf0 = new SimpleDateFormat("HH:mm:ss:SSS");
    SimpleDateFormat sdf1 = new SimpleDateFormat(":SSS");

    // We have a system-wide snapshot: timelines from each Node.  We will be
    // picking one event from all our various timelines at a time.  This means
    // we need a Cursor - a pointer into EACH timeline, and we'll pick and
    // advance one at a time.
    
    // Count all the events to view
    int num_events = 0;
    for( int i=0; i<snapshot.length; i++ )
      num_events += TimeLine.length();

    // Build a time-sorted table of events
    int alt=0;                  // Alternate color per row
    long sec = 0;               // Last second viewed
    long nsec = 0;              // Last nanosecond viewed
    
    ArrayList<TimelineSnapshot.Event> heartbeats = new ArrayList<TimelineSnapshot.Event> (); 
    for(TimelineSnapshot.Event event:events){
      H2ONode h2o = cloud._memary[event.nodeId()];
      
      // The event type.  First get payload.
      long l0 = event.dataLo();
      long h8 = event.dataHi();
      int udp_type = (int)(l0&0xff); // First byte is UDP packet type
      UDP.udp e = UDP.udp.UDPS[udp_type];

      InetAddress inet = event.addrPack();     

      // See if this is a repeated Heartbeat.
      if( e == UDP.udp.heartbeat ){
        heartbeats.add(event);
        continue;        
      } else if(!heartbeats.isEmpty()){        
        int [] sends = new int [TimeLine.CLOUD.size()];
        int [] recvs = new int [TimeLine.CLOUD.size()];
        for(TimelineSnapshot.Event h:heartbeats){
          if(h.isSend()) ++sends[h.nodeId()]; else ++recvs[h.nodeId()];          
        }
        StringBuilder heartBeatStr = new StringBuilder();
        int allSends = 0;
        int allRecvs = 0;
        for(int i = 0; i < sends.length; ++i){
          if(i != 0)heartBeatStr.append(", ");
          heartBeatStr.append(sends[i] + ":" + recvs[i]);
          allSends += sends[i];
          allRecvs += recvs[i];
        }
        long hms = heartbeats.get(heartbeats.size()-1).ms(); // Event happened msec
        long hsec0 = hms/1000;
        String hdate = ((hsec0 == sec) ? sdf1 : sdf0).format(new Date(hms));
        sec = hsec0;
        RString row = response.restartGroup("tableRow");
        row.replace("udp","heartbeat");
        row.replace("msec",hdate);
        row.replace("nsec","lots");
        row.replace("send","many");
        row.replace("recv","many");
        row.replace("bytes", allSends + " sends, " + allRecvs + " recvs (" + heartBeatStr.toString() + ")");
        row.append();
        heartbeats.clear();
      }
      
      // Break down time into something readable
      long ms = event.ms(); // Event happened msec
      long ns = event.ns(); // Event happened nanosec
      long sec0 = ms/1000;         // Round down to nearest second
      String date = ((sec0 == sec) ? sdf1 : sdf0).format(new Date(ms));
      sec = sec0;
      
      // A row for this event
      RString row = response.restartGroup("tableRow");

      row.replace("udp",e.toString());
      row.replace("msec",date);
      row.replace("nsec",((Math.abs(ns-nsec)>2000000)?"lots":(ns-nsec)));
      nsec = ns;

      // Who and to/from
      if( event.isSend()) { // This is a SENT packet
        row.replace("send","<strong>"+h2o+"</strong>"); // sent from self
        if(!inet.isMulticastAddress()){
          int port = -1;
          if(events._sends.containsKey(event) && !events._sends.get(event).isEmpty())
            port = TimeLine.CLOUD._memary[events._sends.get(event).get(0).nodeId()]._key._port;
          String portStr = ":" + ((port != -1)?port:"?");//((port != -1)?port:"?"); 
          String addrString = inet.toString() + portStr;
          row.replace("recv",addrString);
        } else
          row.replace("recv","multicast");
      } else { // Else this is a RECEIVED packet
        // get the sender's port  
        int port = event.portPack();
        row.replace("send",event.addrString());
        row.replace("recv","<strong>"+((inet.equals(h2o._key._inet) && (port == h2o._key._port))?"self":h2o)+"</strong>");
      }
      // Pretty-print payload
      row.replace("bytes",UDP.printx16(l0,h8));      
      row.append();
    }
    
    response.replace("noOfRows",alt);
 
    // Report time to build this page... because building it is slow...
    long now2 = System.currentTimeMillis();
    //System.out.println("took "+(now2-ctm)+"ms to gen html");

    return response.toString();
  }

  final static String html =
           "<div class='alert alert-success'>Snapshot taken: <strong>%now</strong> by <strong>%self</strong></div>"
          + "<p>Showing %noOfRows events\n"
          + "<table class='table table-striped table-bordered table-condensed'>"
          + "<thead><th>hh:mm:ss:ms<th>nanosec<th>who<th>event<th>bytes</thead>\n"
          + "<tbody>"
          + "%tableRow{"
          + "  <tr>"
          + "    <td align=right>%msec</td>"
          + "    <td align=right>+%nsec</td>"
          + "    <td>%send&rarr;%recv</td>"
          + "    <td>%udp</td>"
          + "    <td>%bytes</td>"
          + "  </tr>\n"
          + "}"
          + "</table>"
          ;
}
