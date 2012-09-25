package water.serialization;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import water.RemoteTask;

import com.google.common.base.Throwables;
import com.google.common.cache.*;

public class RTSerializationManager {
  private static final LoadingCache<Class<?>, RemoteTaskSerializer<?>> CACHE =
      CacheBuilder.newBuilder().build(new CacheLoader<Class<?>, RemoteTaskSerializer<?>>() {
        @Override
        public RemoteTaskSerializer<?> load(Class<?> cls) throws Exception {
          if( !RemoteTask.class.isAssignableFrom(cls) ) {
            throw new Error(cls.getName() + " is not a subclass of RemoteTask");
          }
          RemoteTaskSerializer<?> serializer = getCustomSerializer(cls);
          if( serializer != null ) return serializer;
          return buildSerializer(cls);
        }
      });

  @SuppressWarnings("unchecked")
  public static RemoteTaskSerializer<RemoteTask> get(Class<? extends RemoteTask> cls) {
    try {
      return (RemoteTaskSerializer<RemoteTask>) CACHE.get(cls);
    } catch( ExecutionException e ) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static RemoteTaskSerializer<RemoteTask> get(String className) {
    try {
      return get((Class<? extends RemoteTask>) Class.forName(className));
    } catch( ClassNotFoundException e ) {
      throw new RuntimeException(e);
    }
  }

  private static RemoteTaskSerializer<?> getCustomSerializer(Class<?> clazz)
      throws InstantiationException, IllegalAccessException {
    RTSerializer serializerAnnotation = clazz.getAnnotation(RTSerializer.class);
    if( serializerAnnotation == null ) return null;
    Class<?> serializerClazz = serializerAnnotation.value();
    return (RemoteTaskSerializer<?>) serializerClazz.newInstance();
  }

  private static RemoteTaskSerializer<?> buildSerializer(Class<?> cls) throws Exception {
    RTSerGenerator gen = new RTSerGenerator(cls);
    byte[] serializerBytes = gen.createSerializer();
    Class<?> serializerClass = loadClass(cls.getClassLoader(), serializerBytes);
    return (RemoteTaskSerializer<?>) serializerClass.newInstance();
  }

  private static final Method defineClass;
  static {
    try {
      Class<?> cls = Class.forName("java.lang.ClassLoader");
      defineClass = cls.getDeclaredMethod("defineClass", String.class, byte[].class, int.class, int.class);
      defineClass.setAccessible(true);
    } catch( Throwable e ) {
      throw Throwables.propagate(e);
    }
  }
  private static Class<?> loadClass(ClassLoader loader, byte[] b) throws Exception {
    return (Class<?>) defineClass.invoke(loader, null, b, 0, b.length);
  }
}
