package water;

/**
 * A Paxos packet: an Accepted packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPPaxosAccepted extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    Paxos.State t = ab.get(Paxos.State.class);
    Paxos.doAccepted(t, ab._h2o);
    return ab;
  }

  // Build an Accepted packet.  It has the membership, plus proposal
  static void build_and_multicast(Paxos.State state) {
    new AutoBuffer(H2O.SELF).putUdp(udp.paxos_accepted).put(state).close();
  }
}
