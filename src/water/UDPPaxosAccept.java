package water;

/**
 * A Paxos packet: an AcceptRequest packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPPaxosAccept extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    Paxos.State s = ab.get(Paxos.State.class);
    Paxos.doAccept(s, ab._h2o);
    return ab;
  }

  // Build an AcceptRequest packet.  It is our PROPOSAL with the 8 bytes of
  // Accept number set, plus the Value (wireline member protocol)
  static void build_and_multicast( Paxos.State state ) {
    new AutoBuffer(H2O.SELF).putUdp(udp.paxos_accept).put(state).close();
  }
}
