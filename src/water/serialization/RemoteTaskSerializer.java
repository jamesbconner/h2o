package water.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.RemoteTask;

/**
 * A {@link RemoteTaskSerializer} manages writing RemoteTasks to and reading
 * them from raw bytes.
 */
public abstract class RemoteTaskSerializer<T extends RemoteTask> {
  // User overrides these methods to send his results back and forth.
  // Reads & writes user-guts to a line-wire format on a correctly typed object
  abstract public int wire_len(T task);
  abstract public int  write( T task, byte[] buf, int off );
  abstract public void write( T task, DataOutputStream dos ) throws IOException;
  abstract public T read( byte[] buf, int off );
  abstract public T read( DataInputStream dis ) throws IOException;
}
