package water.serialization;

import java.util.concurrent.ExecutionException;

import water.RemoteTask;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class RemoteTaskSerializationManager {
  private static final LoadingCache<Class<?>, RemoteTaskSerializer<?>> CACHE =
      CacheBuilder.newBuilder().build(new CacheLoader<Class<?>, RemoteTaskSerializer<?>>() {
        @Override
        public RemoteTaskSerializer<?> load(Class<?> cls) throws Exception {
          if (!RemoteTask.class.isAssignableFrom(cls)) {
            throw new Error(cls.getName() + " is not a subclass of RemoteTask");
          }
          RemoteTaskSerializer<?> serializer = getCustomSerializer(cls);
          if (serializer != null) return serializer;
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
    if (serializerAnnotation == null) {
      throw new Error("RemoteTask lacks a RTSerializer: " + clazz.getName());
    }
    Class<?> serializerClazz = serializerAnnotation.value();
    return (RemoteTaskSerializer<?>) serializerClazz.newInstance();
  }
  
  private static RemoteTaskSerializer<?> buildSerializer(Class<?> cls) {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

}
