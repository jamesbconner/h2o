package water.serialization;

import java.io.*;
import java.lang.reflect.*;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.*;
import org.objectweb.asm.Type;

import water.RemoteTask;
import water.Stream;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

/**
 * Generates custom {@link RemoteTaskSerializer} for particular
 * {@link RemoteTask}s.
 */
public class RTSerGenerator implements Opcodes {
  private static final Type RT_TYPE = Type.getType(RemoteTask.class);

  private static final Type SER_TYPE;
  private static final Method R_STREAM;
  private static final Method W_STREAM;
  private static final Method R_DATA_STREAM;
  private static final Method W_DATA_STREAM;
  private static final Method R_BYTES;
  private static final Method W_BYTES;
  private static final Method WIRE_LEN;
  static {
    try {
      Class<RemoteTaskSerializer> c = RemoteTaskSerializer.class;
      SER_TYPE = Type.getType(c);
      WIRE_LEN      = c.getDeclaredMethod("wire_len", RemoteTask.class);
      R_STREAM      = c.getDeclaredMethod("read", Stream.class);
      R_DATA_STREAM = c.getDeclaredMethod("read", DataInputStream.class);
      R_BYTES       = c.getDeclaredMethod("read", byte[].class, int.class);
      W_STREAM      = c.getDeclaredMethod("write", RemoteTask.class, Stream.class);
      W_DATA_STREAM = c.getDeclaredMethod("write", RemoteTask.class, DataOutputStream.class);
      W_BYTES       = c.getDeclaredMethod("write", RemoteTask.class, byte[].class, int.class);
    } catch(Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private static final Set<Class<?>> SUPPORTED_CLASSES = new HashSet<Class<?>>();
  static {
    SUPPORTED_CLASSES.add(byte[].class);
    //SUPPORTED_CLASSES.add(int.class);
    //SUPPORTED_CLASSES.add(String.class);
  }

  private final Class<?> clazz;
  private final String internalName;
  private final Field[] fields;
  private final Constructor<?> ctor;

  public RTSerGenerator(Class<?> c) throws SecurityException {
    if (!RemoteTask.class.isAssignableFrom(c)) {
      throw new RuntimeException(MessageFormat.format(
          "{0}: is not a RemoteRunnable",
          c.getName()));
    }
    this.clazz = c;
    this.internalName = Type.getInternalName(c);
    this.fields = c.getDeclaredFields();

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

  private String getSerializerInternalName() {
    return internalName + "Serializer";
  }


  /**
   * Create the RemoteRunnableSerializer.  It will have an inner class of an identical
   * but remapped runnable which it uses to instantiate.
   */
  public byte[] createSerializer() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);

    cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER,
        getSerializerInternalName(),
        null,
        SER_TYPE.getInternalName(),
        new String[] { });

    createConstructor(cw);
    createRead(cw);
    createWrite(cw);
    createDummies(cw);
    cw.visitEnd();
    return cw.toByteArray();
  }

  private void createConstructor(ClassWriter cw) {
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, SER_TYPE.getInternalName(), "<init>", "()V");
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  public void createDummies(ClassWriter cw) {
    for( Method m : new Method[] { WIRE_LEN, R_BYTES, W_BYTES, R_DATA_STREAM, W_DATA_STREAM, R_STREAM, W_STREAM }) {
      System.out.println("Stubbing off: " + m);
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

  private void createRead(ClassWriter cw) {
//    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "read",
//        Type.getMethodDescriptor(REMOTE_RUNNABLE_TYPE, new Type[] {
//            DATA_INPUT_TYPE
//        }), null,
//        new String[] { IO_EXCEPTION_TYPE.getInternalName() });
//    mv.visitCode();
//    mv.visitTypeInsn(NEW, runnableInternalName);
//    mv.visitInsn(DUP);
//    for (Field f : fields) {
//      mv.visitVarInsn(ALOAD, 1);
//      if (int.class.equals(f.getType())) {
//        mv.visitMethodInsn(INVOKEINTERFACE,
//            DATA_INPUT_TYPE.getInternalName(),
//            "readInt", "()I");
//      } else if (String.class.equals(f.getType())) {
//        mv.visitMethodInsn(INVOKEINTERFACE,
//            DATA_INPUT_TYPE.getInternalName(),
//            "readUTF", "()Ljava/lang/String;");
//      } else {
//        throw new RuntimeException("Unhandled field type: " + f.getType());
//      }
//    }
//    String ctorDesc = Type.getConstructorDescriptor(ctor);
//    mv.visitMethodInsn(INVOKESPECIAL, runnableInternalName, "<init>", ctorDesc);
//    mv.visitInsn(ARETURN);
//    mv.visitMaxs(0, 0);
//    mv.visitEnd();
  }

  private void createWrite(ClassWriter cw) {
//    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "write",
//        Type.getMethodDescriptor(Type.VOID_TYPE, new Type[] {
//            REMOTE_RUNNABLE_TYPE, DATA_OUTPUT_TYPE
//        }), null,
//        new String[] { IO_EXCEPTION_TYPE.getInternalName() });
//    mv.visitCode();
//    mv.visitVarInsn(ALOAD, 1);
//    mv.visitTypeInsn(CHECKCAST, runnableInternalName);
//    mv.visitVarInsn(ASTORE, 3);
//    for (Field f : fields) {
//      mv.visitVarInsn(ALOAD, 2);
//      mv.visitVarInsn(ALOAD, 3);
//      mv.visitFieldInsn(GETFIELD, runnableInternalName, f.getName(),
//          Type.getDescriptor(f.getType()));
//      if (int.class.equals(f.getType())) {
//        mv.visitMethodInsn(INVOKEINTERFACE,
//            DATA_OUTPUT_TYPE.getInternalName(),
//            "writeInt", "(I)V");
//      } else if (String.class.equals(f.getType())) {
//        mv.visitMethodInsn(INVOKEINTERFACE,
//            DATA_OUTPUT_TYPE.getInternalName(),
//            "writeUTF", "(Ljava/lang/String;)V");
//      } else {
//        throw new RuntimeException("Unhandled field type: " + f.getType());
//      }
//    }
//    mv.visitInsn(RETURN);
//    mv.visitMaxs(0, 0);
//    mv.visitEnd();
  }
}
