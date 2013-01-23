package test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import water.H2O;

/**
 * Builds a cloud, either as JVMs or isolated class loaders in a JVM, and runs
 * tests. Uses parent JVM settings, so handy for IDEs in particular.
 */
public class TestRunner {
  public static void main(String[] args) throws Exception {
    ArrayList<Separate> sites = new ArrayList<Separate>();
    int nodes = 2;

    for( int i = 0; i < nodes; i++ ) {
      sites.add(SeparateVM.start());
      // sites.add(new SeparateCL());
    }

    // org.junit.runner.JUnitCore.runClasses(KMeansTest.class);

    H2O.main(new String[] {});
    TestUtil.stall_till_cloudsize(nodes + 1);
    new KMeansTest().testGaussian((int) 1e6);

    BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
    console.readLine();

    for( Separate site : sites )
      site.close();

    // TODO proper shutdown of remaining threads?
    System.exit(0);
  }
}
