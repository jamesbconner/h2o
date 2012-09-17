package water.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import water.RemoteTask;

/**
 * Generates custom {@link RemoteRunnableSerializer} for particular
 * {@link RemoteRunnable}s.
 */
public class RTSerGenerator implements Opcodes {
  private static final Type RT_TYPE = Type.getType(RemoteTask.class);
  private static final Type REMOTE_RUNNABLE_SERIALIZER_TYPE = Type.getType(RemoteTaskSerializer.class);

  private static final Set<Class<?>> SUPPORTED_CLASSES = new HashSet<Class<?>>();
  static {
    SUPPORTED_CLASSES.add(int.class);
    SUPPORTED_CLASSES.add(String.class);
  }

  private static Class<?> loadClass(ClassLoader loader, byte[] b) throws Exception {
    Class<?> clazz = null;
    Class<?> cls = Class.forName("java.lang.ClassLoader");
    java.lang.reflect.Method method =
        cls.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class, int.class, int.class });

    method.setAccessible(true);
    try {
      Object[] args = new Object[] { null, b, new Integer(0), new Integer(b.length)};
      clazz = (Class<?>) method.invoke(loader, args);
    } finally {
      method.setAccessible(false);
    }
    return clazz;
  }

  /**
   * The main entry point for everything.  Given a {@link RemoteRunnable} class,
   * generate a custom serializer for it.
   */
  public static <T extends RemoteTask> RemoteTaskSerializer<T> genSerializer(Class<T> c) throws Exception {
    RTSerGenerator gen = new RTSerGenerator(c);
    byte[] serializerBytes = gen.createSerializer();
    Class<?> serializerClass = loadClass(c.getClassLoader(), serializerBytes);
    return (RemoteTaskSerializer<T>) serializerClass.newInstance();
  }

  private final String runnableInternalName;
  private final Field[] fields;
  private final Constructor<?> ctor;

  public RTSerGenerator(Class<?> c) {
    if (!RemoteTask.class.isAssignableFrom(c)) {
      throw new RuntimeException(MessageFormat.format(
          "{0}: is not a RemoteRunnable",
          c.getName()));
    }
    Constructor<?>[] ctors = c.getDeclaredConstructors();
    if (ctors.length != 1) {
      throw new RuntimeException(MessageFormat.format(
          "{0}: unexpected number of constructors",
          c.getName()));
    }

    this.runnableInternalName = Type.getInternalName(c);
    this.ctor = ctors[0];
    this.fields = c.getDeclaredFields();


    Class<?>[] parameterTypes = ctor.getParameterTypes();
    if (fields.length != parameterTypes.length) {
      throw new RuntimeException(MessageFormat.format(
          "{0}: number of fields {1} and number of constructors {2} do not match",
          c.getName(), fields.length, parameterTypes.length));
    }
    for (int i = 0; i < fields.length; ++i) {
      Class<?> fc = fields[i].getType();
      if (!fc.equals(parameterTypes[i])) {
        throw new RuntimeException(MessageFormat.format(
            "{0}: Field {1} has type {2} which does not match parameter {3} of type {4}",
            c.getName(),
            fields[i].getName(),
            fc.getName(),
            i, parameterTypes[i].getName()));
      }
      if (!SUPPORTED_CLASSES.contains(fc)) {
        throw new RuntimeException(MessageFormat.format(
            "{0}: Field {1} has unsupported type {2}",
            c.getName(),
            fields[i].getName(),
            fc.getName()));
      }
    }
  }

  private String getSerializerInternalName() {
    return runnableInternalName + "Serializer";
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
        Type.getInternalName(Object.class),
        new String[] { REMOTE_RUNNABLE_SERIALIZER_TYPE.getInternalName() });

    createConstructor(cw);
    createRead(cw);
    createWrite(cw);
    cw.visitEnd();
    return cw.toByteArray();
  }

  private void createConstructor(ClassWriter cw) {
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private void createRead(ClassWriter cw) {
  }

  private void createWrite(ClassWriter cw) {
  }
}
