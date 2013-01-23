package test;

import init.Boot;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Executes code in a separate VM.
 */
public class SeparateVM implements Separate {

  private final Process _process;

  private SeparateVM(Process process) {
    _process = process;
  }

  public static SeparateVM start(String... args) throws IOException {
    ArrayList<String> list = new ArrayList<String>();
    list.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    list.add("-cp");
    list.add(System.getProperty("java.class.path"));
    // list.add("-agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=y");
    list.add(SeparateVM.class.getName());
    list.addAll(Arrays.asList(args));
    ProcessBuilder builder = new ProcessBuilder(list);
    builder.directory(new File(System.getProperty("user.dir")));
    builder.redirectErrorStream(true);
    Process process = builder.start();
    inheritIO(process);
    return new SeparateVM(process);
  }

  public static void main(String[] args) throws Exception {
    new Thread() {
      @Override
      public void run() {
        for( ;; ) {
          int b;
          try {
            b = System.in.read();
            Thread.sleep(1);
          } catch( Exception e ) {
            b = -1;
          }
          if( b < 0 ) {
            System.out.println("Assuming parent done, exit(0)");
            System.exit(0);
          }
        }
      }
    }.start();

    Boot.main(args);
  }

  @Override
  public void close() {
    _process.destroy();
  }

  public int exitValue() {
    return _process.exitValue();
  }

  @Override
  public void waitForEnd() {
    try {
      _process.waitFor();
    } catch( InterruptedException e ) {
      e.printStackTrace();
    }
  }

  private static final void inheritIO(Process process) {
    // Java 7
    // builder.inheritIO();

    final BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));

    Thread thread = new Thread() {

      @Override
      public void run() {
        try {
          for( ;; ) {
            String line = input.readLine();

            if( line == null )
              break;

            System.out.println(line);
          }
        } catch( IOException e ) {
          e.printStackTrace();
        }
      }
    };

    thread.start();
  }
}