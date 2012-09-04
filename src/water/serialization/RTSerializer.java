package water.serialization;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RTSerializer {
  Class<? extends RemoteTaskSerializer> value();
}
