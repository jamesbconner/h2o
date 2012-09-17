package water.serialization;

import java.lang.instrument.IllegalClassFormatException;

import org.objectweb.asm.*;

import water.RemoteTask;

/**
 * A class for rewriting bytecode of {@link RemoteTask} to force members to be
 * public.  This is a necessary pre-condition for generated automated
 * serialization methods.
 */
public class RTWeaver implements Opcodes {
  private static final String RT_INTERNAL_NAME = Type.getType(RemoteTask.class).getInternalName();
  static { assert !RemoteTask.class.isInterface(); }

  public byte[] transform(ClassLoader loader, String className, byte[] bytes)
          throws IllegalClassFormatException {
    ClassReader reader = new ClassReader(bytes);
    boolean isRunnable = RT_INTERNAL_NAME.equals(reader.getSuperName());
    if( !isRunnable ) return null;

    ClassWriter write = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
    ClassAdapter adapter = new ClassAdapter(write) {
      @Override
      public void visit(int version, int access, String name,
          String signature, String superName, String[] interfaces) {
        super.visit(version, makePublicAccess(access),
            name, signature, superName, interfaces);
      }

      @Override
      public MethodVisitor visitMethod(int access, String name,
          String desc, String signature, String[] exceptions) {
        return super.visitMethod(makePublicAccess(access),
            name, desc, signature, exceptions);
      }

      @Override
      public FieldVisitor visitField(int access, String name,
          String desc, String signature, Object value) {
        return super.visitField(makePublicAccess(access),
            name, desc, signature, value);
      }
    };
    reader.accept(adapter, 0);
    return write.toByteArray();
  }

  private static int makePublicAccess(int access) {
    access &= ~ACC_PRIVATE;
    access &= ~ACC_PROTECTED;
    access |= ACC_PUBLIC;
    return access;
  }
}
