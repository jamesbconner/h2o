package water.serialization;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.*;
import java.text.MessageFormat;
import java.util.*;

import org.objectweb.asm.*;
import org.objectweb.asm.Type;

import water.RemoteTask;
import water.Stream;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Generates custom {@link RemoteTaskSerializer} for particular
 * {@link RemoteTask}s.
 */
public class RTSerGenerator implements Opcodes {
  private static final Type   SER;
  private static final Method SER_R_STREAM;
  private static final Method SER_W_STREAM;
  private static final Method SER_R_DATA_STREAM;
  private static final Method SER_W_DATA_STREAM;
  private static final Method SER_R_BYTES;
  private static final Method SER_W_BYTES;
  private static final Method SER_WIRE_LEN;
  static {
    try {
      Class<RemoteTaskSerializer> c = RemoteTaskSerializer.class;
      SER = Type.getType(c);
      SER_WIRE_LEN      = c.getDeclaredMethod("wire_len", RemoteTask.class);
      SER_R_STREAM      = c.getDeclaredMethod("read", Stream.class);
      SER_R_DATA_STREAM = c.getDeclaredMethod("read", DataInputStream.class);
      SER_R_BYTES       = c.getDeclaredMethod("read", byte[].class, int.class);
      SER_W_STREAM      = c.getDeclaredMethod("write", RemoteTask.class, Stream.class);
      SER_W_DATA_STREAM = c.getDeclaredMethod("write", RemoteTask.class, DataOutputStream.class);
      SER_W_BYTES       = c.getDeclaredMethod("write", RemoteTask.class, byte[].class, int.class);
    } catch(Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private static final Type STREAM;
  private static final Method STREAM_SET_BYTES;
  private static final Method STREAM_GET_BYTES;
  private static final Method STREAM_SET4;
  private static final Method STREAM_GET4;
  static {
    try {
      Class<Stream> c = Stream.class;
      STREAM = Type.getType(c);
      STREAM_SET_BYTES   = c.getDeclaredMethod("setLen4Bytes", byte[].class);
      STREAM_GET_BYTES   = c.getDeclaredMethod("getLen4Bytes");
      STREAM_SET4        = c.getDeclaredMethod("set4", int.class);
      STREAM_GET4        = c.getDeclaredMethod("get4");
    } catch(Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private static final Type   DOS;
  private static final Method DOS_WRITE_INT;
  private static final Method DOS_WRITE;

  private static final Type   DIS;
  private static final Method DIS_READ_INT;
  private static final Method DIS_READ_FULLY;
  static {
    try {
      Class<DataOutputStream> dos = DataOutputStream.class;
      DOS            = Type.getType(dos);
      DOS_WRITE_INT  = dos.getMethod("writeInt", int.class);
      DOS_WRITE      = dos.getMethod("write", byte[].class);

      Class<DataInputStream> dis = DataInputStream.class;
      DIS            = Type.getType(dis);
      DIS_READ_INT   = dis.getMethod("readInt");
      DIS_READ_FULLY = dis.getMethod("readFully", byte[].class);
    } catch(Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private static final Set<Class<?>> SUPPORTED_CLASSES = new HashSet<Class<?>>();
  static {
    SUPPORTED_CLASSES.add(byte[].class);
    SUPPORTED_CLASSES.add(int.class);
    //SUPPORTED_CLASSES.add(String.class);
  }

  private final String internalName;
  private final Field[] fields;
  private final Constructor<?> ctor;

  public RTSerGenerator(Class<?> c) throws SecurityException {
    if (!RemoteTask.class.isAssignableFrom(c)) {
      throw new RuntimeException(MessageFormat.format(
          "{0}: is not a RemoteRunnable",
          c.getName()));
    }
    this.internalName = Type.getInternalName(c);

    ArrayList<Field> fi = Lists.newArrayList(c.getDeclaredFields());
    Iterator<Field> it = fi.iterator();
    while( it.hasNext() ) {
      if( Modifier.isStatic(it.next().getModifiers()) ) it.remove();
    }
    this.fields = fi.toArray(new Field[fi.size()]);

    Class<?>[] fieldTypes = new Class<?>[fields.length];
    for( int i = 0; i < fields.length; ++i ) {
      fieldTypes[i] = fields[i].getType();
      if (!SUPPORTED_CLASSES.contains(fieldTypes[i])) {
        throw new RuntimeException(MessageFormat.format(
            "{0}: Field {1} has unsupported type {2}",
            c.getName(),
            fields[i].getName(),
            fieldTypes[i]));
      }
    }
    try {
      this.ctor = c.getDeclaredConstructor(fieldTypes);
    } catch( NoSuchMethodException e ) {
      throw new RuntimeException(MessageFormat.format(
          "{0}: No constructor for field types: {1}",
          c.getName(), Joiner.on(", ").join(fieldTypes)), e);
    }
  }

  /**
   * Create the RemoteRunnableSerializer.  It will have an inner class of an identical
   * but remapped runnable which it uses to instantiate.
   */
  public byte[] createSerializer() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);

    cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER,
        internalName + "Serializer",
        null,
        SER.getInternalName(),
        new String[] { });

    createConstructor(cw);
    createWireLen(cw);
    createWriteStream(cw);
    createReadStream(cw);
    createWriteDataStream(cw);
    createReadDataStream(cw);
    createDummies(cw);
    cw.visitEnd();
    return cw.toByteArray();
  }

  public void createDummies(ClassWriter cw) {
    for( Method m : new Method[] { SER_R_BYTES, SER_W_BYTES }) {
      MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, m.getName(), Type.getMethodDescriptor(m), null, new String[0]);
      mv.visitCode();
      mv.visitTypeInsn(NEW, Type.getInternalName(RuntimeException.class));
      mv.visitInsn(DUP);
      mv.visitMethodInsn(INVOKESPECIAL, Type.getInternalName(RuntimeException.class), "<init>", "()V");
      mv.visitInsn(ATHROW);
      mv.visitMaxs(0, 0);
      mv.visitEnd();
    }
  }

  private void createConstructor(ClassWriter cw) {
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, SER.getInternalName(), "<init>", "()V");
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  public void createWireLen(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_WIRE_LEN);
    int casted = visitDowncast(mv, SER_WIRE_LEN);

    // total = 0
    mv.visitInsn(ICONST_0);
    for( Field f : fields ) {
      if( byte[].class.equals(f.getType()) ) {
        // total += array.length + 4;
        visitField(mv, casted, f);
        mv.visitInsn(ARRAYLENGTH);
        mv.visitInsn(IADD);
        mv.visitInsn(ICONST_4);
        mv.visitInsn(IADD);
      } else if( int.class.equals(f.getType()) ) {
        // total += 4
        mv.visitInsn(ICONST_4);
        mv.visitInsn(IADD);
      } else {
        throw new Error("Unimplemented field type: " + f.getType());
      }
    }
    // return the cumulative total
    visitReturn(mv, IRETURN);
  }

  public void createWriteStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_W_STREAM);
    int casted = visitDowncast(mv, SER_W_STREAM);
    int stream = 2;

    for( Field f : fields ) {
      if( byte[].class.equals(f.getType()) ) {
        // stream.setLen4Bytes(array)
        mv.visitIntInsn(ALOAD, stream);
        visitField(mv, casted, f);
        visitMethodCall(mv, STREAM, STREAM_SET_BYTES);
        mv.visitInsn(POP);
      } else if( int.class.equals(f.getType()) ) {
        // stream.set4(f)
        mv.visitIntInsn(ALOAD, stream);
        visitField(mv, casted, f);
        visitMethodCall(mv, STREAM, STREAM_SET4);
        mv.visitInsn(POP);
      } else {
        throw new Error("Unimplemented field type: " + f.getType());
      }
    }
    visitReturn(mv, RETURN);
  }

  public void createReadStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_R_STREAM);
    int stream = 1;
    mv.visitTypeInsn(NEW, internalName);
    mv.visitInsn(DUP);

    for( Field f : fields ) {
      if( byte[].class.equals(f.getType()) ) {
        // stream.getLen4Bytes()
        mv.visitIntInsn(ALOAD, stream);
        visitMethodCall(mv, STREAM, STREAM_GET_BYTES);
      } else if( int.class.equals(f.getType()) ) {
        // stream.get4()
        mv.visitIntInsn(ALOAD, stream);
        visitMethodCall(mv, STREAM, STREAM_GET4);
      } else {
        throw new Error("Unimplemented field type: " + f.getType());
      }
    }
    mv.visitMethodInsn(INVOKESPECIAL, internalName, "<init>", Type.getConstructorDescriptor(ctor));
    visitReturn(mv, ARETURN);
  }

  public void createWriteDataStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_W_DATA_STREAM);
    int casted = visitDowncast(mv, SER_W_DATA_STREAM);
    int stream = 2;

    for( Field f : fields ) {
      if( byte[].class.equals(f.getType()) ) {
        // dos.writeInt(array.length)
        mv.visitIntInsn(ALOAD, stream);
        visitField(mv, casted, f);
        mv.visitInsn(ARRAYLENGTH);
        visitMethodCall(mv, DOS, DOS_WRITE_INT);

        // dos.write(array)
        mv.visitIntInsn(ALOAD, stream);
        visitField(mv, casted, f);
        visitMethodCall(mv, DOS, DOS_WRITE);
      } else if( int.class.equals(f.getType()) ) {
        // dos.writeInt(f)
        mv.visitIntInsn(ALOAD, stream);
        visitField(mv, casted, f);
        visitMethodCall(mv, DOS, DOS_WRITE_INT);
      } else {
        throw new Error("Unimplemented field type: " + f.getType());
      }
    }
    visitReturn(mv, RETURN);
  }

  public void createReadDataStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_R_DATA_STREAM);
    int stream = 1;
    mv.visitTypeInsn(NEW, internalName);
    mv.visitInsn(DUP);

    for( Field f : fields ) {
      if( byte[].class.equals(f.getType()) ) {
        // new byte[dis.readInt()]
        mv.visitIntInsn(ALOAD, stream);
        visitMethodCall(mv, DIS, DIS_READ_INT);
        mv.visitIntInsn(NEWARRAY, T_BYTE);

        // dup array for our ctor, then `dis.readFull(array)`
        mv.visitInsn(DUP);
        mv.visitIntInsn(ALOAD, stream);
        mv.visitInsn(SWAP);
        visitMethodCall(mv, DIS, DIS_READ_FULLY);
      } else if( int.class.equals(f.getType()) ) {
        // dos.readInt()
        mv.visitIntInsn(ALOAD, stream);
        visitMethodCall(mv, DIS, DIS_READ_INT);
     } else {
        throw new Error("Unimplemented field type: " + f.getType());
      }
    }
    mv.visitMethodInsn(INVOKESPECIAL, internalName, "<init>", Type.getConstructorDescriptor(ctor));
    visitReturn(mv, ARETURN);
  }

  /** Push a field onto the stack: [] -> [Field] */
  private void visitField(MethodVisitor mv, int varNum, Field f) {
    mv.visitVarInsn(ALOAD, varNum);
    mv.visitFieldInsn(GETFIELD, internalName, f.getName(), Type.getDescriptor(f.getType()));
  }

  /** Start visiting an override for a base class method */
  private MethodVisitor visitDeclareMethod(ClassWriter cw, Method m) {
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC,
        m.getName(), Type.getMethodDescriptor(m),
        null, new String[0]);
    mv.visitCode();
    return mv;
  }

  /** visit instructions to in-place downcast the RemoteTask: [] -> [] */
  private int visitDowncast(MethodVisitor mv, Method m) {
    mv.visitVarInsn(ALOAD, 1);
    mv.visitTypeInsn(CHECKCAST, internalName);
    mv.visitVarInsn(ASTORE, 1);
    return 1;
  }

  /** visit a virtual method call: [args*] -> [return_val] */
  private void visitMethodCall(MethodVisitor mv, Type t, Method m) {
    mv.visitMethodInsn(INVOKEVIRTUAL, t.getInternalName(),
        m.getName(), Type.getMethodDescriptor(m));
  }

  /** visit a final return method and close off the MethodVisitor */
  private void visitReturn(MethodVisitor mv, int ins) {
    assert ins == IRETURN || ins == LRETURN || ins == RETURN || ins == ARETURN;
    mv.visitInsn(ins);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }
}
