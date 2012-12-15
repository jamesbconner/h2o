package water;

/**
 * A Paxos packet: a Promise
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPPaxosPromise extends UDP {
  @Override AutoBuffer call(AutoBuffer ab) {
    Paxos.doPromise(ab.get(Paxos.State.class), ab._h2o);
    return ab;
  }

  static int singlecast(Paxos.State state, H2ONode leader) {
    new AutoBuffer(leader).putUdp(udp.paxos_promise).put(state).close();
    return 0;
  }
}
