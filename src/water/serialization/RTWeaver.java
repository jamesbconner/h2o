package water.serialization;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import org.objectweb.asm.*;

import water.RemoteTask;

import com.google.common.base.Throwables;

/**
 * A class for rewriting bytecode of {@link RemoteTask} to force members to be
 * public.  This is a necessary pre-condition for generated automated
 * serialization methods.
 */
public class RTWeaver implements Opcodes, ClassFileTransformer {
  @Override
  public byte[] transform(ClassLoader loader, String className,
      Class<?> redefiningClass, ProtectionDomain domain, byte[] bytes)
          throws IllegalClassFormatException {
    try {
      return transform(loader, className, bytes);
    } catch (Throwable t) {
      // exceptions in this method get eaten, so we have to be loud
      t.printStackTrace();
      throw Throwables.propagate(t);
    }
  }

  public static byte[] transform(ClassLoader loader, String className, byte[] bytes)
          throws IllegalClassFormatException, ClassNotFoundException {
    ClassReader reader = new ClassReader(bytes);
    String superName = Type.getObjectType(reader.getSuperName()).getClassName();
    Class<?> superClazz = Class.forName(superName,
        false, // DO NOT INIT HERE, we are inside a VM agent if we force the init here we can get into weird circularity errors
        loader);
    if( !RemoteTask.class.isAssignableFrom(superClazz) ) return null;

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
    access &= ~Opcodes.ACC_PRIVATE;
    access &= ~Opcodes.ACC_PROTECTED;
    access |=  Opcodes.ACC_PUBLIC;
    return access;
  }
}
