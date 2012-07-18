/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.web;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
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
    int []idxs = new int[snapshot.length];
    // Count all the events to view
    int num_events = 0;
    for( int i=0; i<snapshot.length; i++ )
      num_events += TimeLine.length();

    // Build a time-sorted table of events
    int alt=0;                  // Alternate color per row
    long sec = 0;               // Last second viewed
    long nsec = 0;              // Last nanosecond viewed

    boolean repeat_beat=false;  // First of a series of heartbeats
    int hb_self = 0;            // Self heartbeats
    int hb_other = 0;           // Other heartbeats

    for( int i=0; i<num_events; i++ ) {
      // Pick the next most-likely event to view - and the Node we are viewing from.
      int node_idx;
      try { 
        node_idx = pick(cloud,snapshot,idxs);
      } catch( ArrayIndexOutOfBoundsException e ) {
        break;
      }
      // Get Node & timeline for that Node.
      H2ONode h2o = cloud._memary[node_idx];
      long[] tl = snapshot[node_idx];
      int idx = idxs[node_idx]; // Index into the timeline buffer
      idxs[node_idx]++;         // Bump the event-viewed index for that Node

      // For a new Snapshot, most of initial entries are all zeros.  Skip them
      // until we start finding entries... which will be the oldest entries.
      // The timeline is age-ordered (per-thread, we hope the threads are
      // fairly consistent)
      if( TimeLine.isEmpty(tl,idx) ) continue;

      // The event type.  First get payload.
      long l0 = TimeLine.l0(tl,idx);
      long h8 = TimeLine.l8(tl,idx);
      int udp_type = (int)(l0&0xff); // First byte is UDP packet type
      UDP.udp e = UDP.udp.UDPS[udp_type];

      InetAddress inet = TimeLine.inet(tl,idx);

      // Break down time into something readable
      long ms = TimeLine.ms(tl,idx); // Event happened msec
      long ns = TimeLine.ns(tl,idx); // Event happened nanosec
      long sec0 = ms/1000;         // Round down to nearest second
      String date = ((sec0 == sec) ? sdf1 : sdf0).format(new Date(ms));
      sec = sec0;

      // See if this is a repeated Heartbeat.
      if( e == UDP.udp.heartbeat ) {
        int node_idx1 = pick(cloud,snapshot,idxs);
        long[] tl1 = snapshot[node_idx];
        int idx1 = idxs[node_idx]; // Index into the timeline buffer
        // Is the next record ALSO a heartbeat?
        if( i < num_events &&
            (TimeLine.l0(tl1,idx1)&0xFF)==UDP.udp.heartbeat.ordinal() ) {
          // We have at least 2 heartbeats in a row
          if( !repeat_beat ) {      // No series yet
            repeat_beat = true;     // Record 1st heartbeat of a series
            hb_self = hb_other = 0; // Reset
          }
          if( inet.equals(H2O.SELF._key._inet) || inet.isMulticastAddress() ) hb_self++;
          else hb_other++;
          continue;                      // Skip this guy for now
        } else {                         // No Heartbeat follows this one
          if( repeat_beat ) {            // Was this a series of heartbeats?
            if( inet.equals(H2O.SELF._key._inet) || inet.isMulticastAddress() ) hb_self++;
            else hb_other++;
            RString row = response.restartGroup("tableRow");
            row.replace("udp",e.toString());
            row.replace("msec",date);
            row.replace("nsec","lots");
            nsec = ns;
            row.replace("recv","many");
            row.replace("send","many");
            row.replace("bytes","Self: "+hb_self+" other: "+hb_other);
            row.append();
            repeat_beat = false;
            continue;
          }
        }
      }

      // A row for this event
      RString row = response.restartGroup("tableRow");

      row.replace("udp",e.toString());
      row.replace("msec",date);
      row.replace("nsec",((Math.abs(ns-nsec)>2000000)?"lots":(ns-nsec)));
      nsec = ns;

      // Who and to/from
      if( TimeLine.send_recv(tl,idx) == 0 ) { // This is a SENT packet
        row.replace("send","<strong>"+h2o+"</strong>"); // sent from self
        row.replace("recv",inet.isMulticastAddress()?"multicast":inet);
      } else {                  // Else this is a RECEIVED packet
        row.replace("send",inet);
        row.replace("recv","<strong>"+(inet.equals(h2o._key._inet)?"self":h2o)+"</strong>");
      }

      // Pretty-print payload
      row.replace("bytes",UDP.printx16(l0,h8));
      row.append();
    }
    
    response.replace("noOfRows",alt);
    _send_host = _send_pack = null;

    // Report time to build this page... because building it is slow...
    long now2 = System.currentTimeMillis();
    System.out.println("took "+(now2-ctm)+"ms to gen html");

    return response.toString();
  }

  // -----
  // Pick the next-most-likely event.  'snapshot' is an array of timelines, 1
  // per Node.  'idxs' is an array of indices into each timeline.
  static int pick( H2O cloud, long[][] snapshot, int [] idxs ) {
    int best_node_idx = 0;
    for( int i=1; i<idxs.length; i++ )
      if( cmp(cloud,snapshot,idxs,best_node_idx,i) == 1 )
        best_node_idx = i;
    // Are we about to pick a 'send' packet?  Hopefully all the receivers
    // (either 1, or everybody for a multicast) are all lined up.  Remember
    // that we just picked this 'send' packet, and choose matching recvs.
    long[]tl0 = snapshot[best_node_idx];
    int  idx0 =     idxs[best_node_idx];
    if( TimeLine.send_recv(tl0,idx0) == 0 ) {
      _send_l0 = TimeLine.l0(tl0,idx0); // Load initial payloads
      _send_h0 = TimeLine.l8(tl0,idx0);
      _send_host = cloud._memary[best_node_idx]._key._inet;
      _send_pack = TimeLine.inet(tl0,idx0);
    }
    return best_node_idx;
  }

  // Some junky static vars to help sort events.  Yah, yah, not thread-safe I
  // know but it'll only mess up the display, and only if 2 people ask for
  // snapshot viewing at the same instant in time, on the same Node.
  static long _send_l0, _send_h0;
  static InetAddress _send_host, _send_pack;

  // Sort 2 events.
  // Return 0 for 0th choice, 1 for the 1st choice
  static int cmp( H2O cloud, long[][] snapshot, int[] idxs, int hidx0, int hidx1 ) {
    long[]tl0 = snapshot[hidx0];
    int  idx0 =     idxs[hidx0];
    long[]tl1 = snapshot[hidx1];
    int  idx1 =     idxs[hidx1];

    // If out of events, pick the other guy
    if( idx0 == TimeLine.length() ) return 1;
    if( idx1 == TimeLine.length() ) return 0;
    // If event is empty, pick the empty event.  We want to flush them out
    // fast, so we can start comparing real events
    if( TimeLine.isEmpty(tl0,idx0) ) return 0;
    if( TimeLine.isEmpty(tl1,idx1) ) return 1;
    // We have 2 non-empty events.  Generally, picking on time only works if
    // both systems have very nearly equal clocks.  Look for obvious
    // happens-before patterns... but only for "close" clocks
    long l0 = TimeLine.l0(tl0,idx0); // Load initial payloads
    long h0 = TimeLine.l8(tl0,idx0);
    long l1 = TimeLine.l0(tl1,idx1);
    long h1 = TimeLine.l8(tl1,idx1);
    // Get times
    long ms0 = TimeLine.ms(tl0,idx0);
    long ms1 = TimeLine.ms(tl1,idx1);
    // Get IP of host: packet sender/receiver
    InetAddress inet_host0 = cloud._memary[hidx0]._key._inet;
    InetAddress inet_host1 = cloud._memary[hidx1]._key._inet;
    // Get send/recv bits
    int sr0 = TimeLine.send_recv(tl0,idx0);
    int sr1 = TimeLine.send_recv(tl1,idx1);
    // Get IP of packet
    InetAddress inet_pack0 = TimeLine.inet(tl0,idx0);
    InetAddress inet_pack1 = TimeLine.inet(tl1,idx1);

    // Pick self-as-target first.  These always come hard on the heels of a
    // just-prior multicast (because you see your own multicast instantly).
    if( inet_host0.equals(inet_pack0) ) return 0;
    if( inet_host1.equals(inet_pack1) ) return 1;

    // Look for 'recv' packets that match the most recent 'send'.
    if( _send_host != null ) {
      if( sr0 == 1 &&
          match( _send_host, _send_pack, _send_l0, _send_h0, inet_host0, inet_pack0, l0, h0) )
        return 0;
      if( sr1 == 1 &&
          match( _send_host, _send_pack, _send_l0, _send_h0, inet_host1, inet_pack1, l1, h1) )
        return 1;
    }

    // Look for 'send' packets with a ready 'recv' packet.
    // Sorta by definition, if a sender is a multicast, and any of the other
    // ready nodes is not a receive of the cast packet, we're not ready.
    if( sr0 == 0 && !inet_pack0.isMulticastAddress() ) {
      throw new Error("unimplemented");
      // Get the next ready packet for the receiver of the 'send' packet
      //int pnidx0 = cloud.nidx(H2ONode.intern(inet_pack0)); // pack0 node index
      //long[]ptl0 = snapshot[pnidx0]; // Ready-packet for receiver
      //int  pidx0 =     idxs[pnidx0];
      //if( TimeLine.send_recv(ptl0,pidx0) == 1 ) {       // It is a receieve packet
      //  InetAddress inet_packpack0 = TimeLine.inet(ptl0,pidx0);
      //  long pl0 = TimeLine.l0(ptl0,pidx0);
      //  long ph0 = TimeLine.l8(ptl0,pidx0);
      //  if( match( inet_host0, inet_pack0, l0, h0, inet_pack0, inet_packpack0, pl0, ph0) )
      //    return 0;
      //}
    }
    if( sr1 == 0 && !inet_pack1.isMulticastAddress() ) {
      throw new Error("unimplemented");
      //// Get the next ready packet for the receiver of the 'send' packet
      //int pnidx0 = cloud.nidx(H2ONode.intern(inet_pack1)); // pack1 node index
      //long[]ptl0 = snapshot[pnidx0]; // Ready-packet for receiver
      //int  pidx0 =     idxs[pnidx0];
      //if( TimeLine.send_recv(ptl0,pidx0) == 1 ) {       // It is a receieve packet
      //  InetAddress inet_packpack0 = TimeLine.inet(ptl0,pidx0);
      //  long pl0 = TimeLine.l0(ptl0,pidx0);
      //  long ph0 = TimeLine.l8(ptl0,pidx0);
      //  if( match( inet_host0, inet_pack0, l0, h0, inet_pack0, inet_packpack0, pl0, ph0) )
      //    return 1;
      //}
    }

    // Look for 'send' packets.  If we see one, stall until the other guy is a
    // matching 'recv' packet.  Include multi-cast and point-to-point.
    if( sr0 == 0 && sr1 == 1 && // send packet / receive packet
        match(inet_host0,inet_pack0,l0,h0, inet_host1,inet_pack1,l1,h1) )
      return 0;
    if( sr1 == 0 && sr0 == 1 && // send packet / receive packet
        match(inet_host1,inet_pack1,l1,h1, inet_host0,inet_pack0,l0,h0) )
      return 1;

    // Pick reboot'ds over other stuff
    int byte00 = (int)(l0&0xFF);
    int byte01 = (int)(l1&0xFF);
    final int reboot = UDP.udp.rebooted.ordinal();
    if( byte00 == reboot && byte01 != reboot ) return 0;
    if( byte01 == reboot && byte00 != reboot ) return 1;
    
    // Avoid receives until a matching send appears
    if( sr0 == 1 && sr1 == 0 ) return 1;
    if( sr1 == 1 && sr0 == 0 ) return 0;
    
    // Pick a heartbeats over other stuff, just to help canonicalize order
    final int hbeat = UDP.udp.heartbeat.ordinal();
    if( byte00 == hbeat && byte01 != hbeat ) return 0;
    if( byte01 == hbeat && byte00 != hbeat ) return 1;

    // Pick the earlier reported time.
    if( ms0 < ms1 ) return 0;
    if( ms0 > ms1 ) return 1;
    
    // Ties pick zero
    return 0;
  }

  // True if these two packets are a matching pair, from sender to receiver
  static boolean match( InetAddress send, InetAddress send_pack, long l0, long h0, 
                        InetAddress recv, InetAddress recv_pack, long l1, long h1 ) {
    return l0==l1 && h0==h1 &&  // matching payloads
      send.equals(recv_pack) && // matching send->recv target?
      (send_pack.equals(recv) ||        // matching recv<-send target?
       send_pack.isMulticastAddress()); // or multi-cast target?
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
  
//  final static RString response = new RString(html);
  
  
}
