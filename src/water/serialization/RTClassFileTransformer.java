package water.serialization;

import init.Loader;

import java.lang.instrument.*;
import java.lang.reflect.Method;
import java.security.ProtectionDomain;

import water.RemoteTask;

/**
 * A class for rewriting bytecode of {@link RemoteTask} to force members to be
 * public.  This is a necessary pre-condition for generated automated
 * serialization methods.
 */
public class RTClassFileTransformer implements ClassFileTransformer {
  // we can be loaded in two different ways.  Handle them.
  public static void premain  (String args, Instrumentation ins) { ins.addTransformer(new RTClassFileTransformer(), true); }
  public static void agentmain(String args, Instrumentation ins) { ins.addTransformer(new RTClassFileTransformer(), true); }

  @Override
  public byte[] transform(ClassLoader loader, String className,
      Class<?> redefiningClass, ProtectionDomain domain, byte[] bytes)
          throws IllegalClassFormatException {
    try {
      Class<?> weaver = Class.forName("water.serialization.RTWeaver", true, Loader.instance());
      Method transform = weaver.getMethod("transform", ClassLoader.class, String.class, byte[].class);
      return (byte[]) transform.invoke(null, loader, className, bytes);
    } catch (Throwable t) {
      // exceptions in this method get eaten, so we have to be loud
      t.printStackTrace();
      throw new RuntimeException(t);
    }
  }
}
