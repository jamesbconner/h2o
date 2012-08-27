package water.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.RemoteTask;

/**
 * A {@link RemoteTaskSerializer} manages writing RemoteTasks to and reading
 * them from raw bytes.
 */
public abstract class RemoteTaskSerializer {
  // User overrides these methods to send his results back and forth.
  // Reads & writes user-guts to a line-wire format on a correctly typed object
  abstract public int wire_len(RemoteTask task);
  abstract public int  write( RemoteTask task, byte[] buf, int off );
  abstract public void write( RemoteTask task, DataOutputStream dos ) throws IOException;
  abstract public RemoteTask read( byte[] buf, int off );
  abstract public RemoteTask read( DataInputStream dis ) throws IOException;
}
