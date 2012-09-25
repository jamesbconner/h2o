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
import com.google.common.collect.*;

/**
 * Generates custom {@link RemoteTaskSerializer} for particular
 * {@link RemoteTask}s.
 */
public class RTSerGenerator implements Opcodes {
  private static final Type HELPER = Type.getType(RTSerGenHelpers.class);
  private static final Set<Class<?>> SUPPORTED_CLASSES = Sets.newHashSet();

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

      SUPPORTED_CLASSES.addAll(RTSerGenHelpers.SUFFIX.keySet());
    } catch(Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private final String internalName;
  private final Field[] fields;
  private final Constructor<?> ctor;
  private final boolean _noArgCtor;

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

    boolean ctorRequiresArgs = false;
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
      ctorRequiresArgs |= Modifier.isFinal(fields[i].getModifiers());
    }
    Constructor<?> ctor = null;
    try {
      ctor = c.getDeclaredConstructor(fieldTypes);
    } catch( NoSuchMethodException e ) {
      if(ctorRequiresArgs) {
        throw new RuntimeException(MessageFormat.format(
            "{0}: No constructor for field types: {1}",
            c.getName(), Joiner.on(", ").join(fieldTypes)), e);
      }
    }
    if(ctor == null) {
      try {
        ctor = c.getDeclaredConstructor();
      } catch( NoSuchMethodException e ) {
        throw new RuntimeException(MessageFormat.format(
            "{0}: No empty constructor or constructor for field types: {1}",
            c.getName(), Joiner.on(", ").join(fieldTypes)), e);
      }
    }
    this.ctor = ctor;
    this._noArgCtor = ctor.getParameterTypes().length == 0;
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
    mv.visitInsn(ICONST_0);
    for( Field f : fields ) {
      visitGetField(mv, casted, f);
      visitHelperCall(mv, RTSerGenHelpers.len(f.getType()));
      mv.visitInsn(IADD);
    }
    visitReturn(mv, IRETURN);
  }

  public void createWriteStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_W_STREAM);
    int casted = visitDowncast(mv, SER_W_STREAM);
    int stream = 2;

    for( Field f : fields ) {
      mv.visitIntInsn(ALOAD, stream);
      visitGetField(mv, casted, f);
      visitHelperCall(mv, RTSerGenHelpers.write(Stream.class, f.getType()));
    }
    visitReturn(mv, RETURN);
  }

  public void createReadStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_R_STREAM);
    int stream = 1;
    mv.visitTypeInsn(NEW, internalName);
    mv.visitInsn(DUP);
    if( _noArgCtor ) {
      mv.visitMethodInsn(INVOKESPECIAL, internalName, "<init>", Type.getConstructorDescriptor(ctor));
      mv.visitVarInsn(ASTORE, 2);
    }

    for( Field f : fields ) {
      if( _noArgCtor ) mv.visitVarInsn(ALOAD, 2);
      mv.visitIntInsn(ALOAD, stream);
      visitHelperCall(mv, RTSerGenHelpers.read(Stream.class, f.getType()));
      if( _noArgCtor ) visitSetField(mv, f);
    }
    if( _noArgCtor ) mv.visitVarInsn(ALOAD, 2);
    else mv.visitMethodInsn(INVOKESPECIAL, internalName, "<init>", Type.getConstructorDescriptor(ctor));
    visitReturn(mv, ARETURN);
  }

  public void createWriteDataStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_W_DATA_STREAM);
    int casted = visitDowncast(mv, SER_W_DATA_STREAM);
    int stream = 2;

    for( Field f : fields ) {
      mv.visitIntInsn(ALOAD, stream);
      visitGetField(mv, casted, f);
      visitHelperCall(mv, RTSerGenHelpers.write(DataOutputStream.class, f.getType()));
    }
    visitReturn(mv, RETURN);
  }

  public void createReadDataStream(ClassWriter cw) {
    MethodVisitor mv = visitDeclareMethod(cw, SER_R_DATA_STREAM);
    int stream = 1;
    mv.visitTypeInsn(NEW, internalName);
    mv.visitInsn(DUP);
    if( _noArgCtor ) {
      mv.visitMethodInsn(INVOKESPECIAL, internalName, "<init>", Type.getConstructorDescriptor(ctor));
      mv.visitVarInsn(ASTORE, 2);
    }

    for( Field f : fields ) {
      if( _noArgCtor ) mv.visitVarInsn(ALOAD, 2);
      mv.visitIntInsn(ALOAD, stream);
      visitHelperCall(mv, RTSerGenHelpers.read(DataInputStream.class, f.getType()));
      if( _noArgCtor ) visitSetField(mv, f);
    }
    if( _noArgCtor ) mv.visitVarInsn(ALOAD, 2);
    else mv.visitMethodInsn(INVOKESPECIAL, internalName, "<init>", Type.getConstructorDescriptor(ctor));
    visitReturn(mv, ARETURN);
  }

  /** Push a field onto the stack: [] -> [Field] */
  private void visitGetField(MethodVisitor mv, int varNum, Field f) {
    mv.visitVarInsn(ALOAD, varNum);
    mv.visitFieldInsn(GETFIELD, internalName, f.getName(), Type.getDescriptor(f.getType()));
  }

  /** set a field from the stack: [obj, field] -> [] */
  private void visitSetField(MethodVisitor mv, Field f) {
    mv.visitFieldInsn(PUTFIELD, internalName, f.getName(), Type.getDescriptor(f.getType()));
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
  private void visitHelperCall(MethodVisitor mv, Method m) {
    mv.visitMethodInsn(INVOKESTATIC, HELPER.getInternalName(),
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
