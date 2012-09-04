package water.parser;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.google.common.base.Throwables;

public class FloatingDecimalWrapper {
  static final Class<sun.misc.FloatingDecimal> CLAZZ;
  static final Constructor<sun.misc.FloatingDecimal> CTOR;
  static final Field IS_NEGATIVE;
  static final Field N_DIGITS;
  static final Field DEC_EXPONENT;
  static final Field DIGITS;
  static final Method DOUBLE_VALUE;
  static final Method FLOAT_VALUE;
  static {
    try {
      CLAZZ = sun.misc.FloatingDecimal.class;
      CTOR = CLAZZ.getDeclaredConstructor(boolean.class, int.class, char[].class, int.class, boolean.class);
      CTOR.setAccessible(true);
      IS_NEGATIVE = CLAZZ.getDeclaredField("isNegative");
      IS_NEGATIVE.setAccessible(true);
      N_DIGITS = CLAZZ.getDeclaredField("nDigits");
      N_DIGITS.setAccessible(true);
      DEC_EXPONENT = CLAZZ.getDeclaredField("decExponent");
      DEC_EXPONENT.setAccessible(true);
      DIGITS = CLAZZ.getDeclaredField("digits");
      DIGITS.setAccessible(true);
      DOUBLE_VALUE = CLAZZ.getDeclaredMethod("doubleValue");
      DOUBLE_VALUE.setAccessible(true);
      FLOAT_VALUE = CLAZZ.getDeclaredMethod("floatValue");
      FLOAT_VALUE.setAccessible(true);
    } catch (Exception e) { throw Throwables.propagate(e); }
  }
  
  private final Object _floatingDecimal;
  boolean isNegative;
  int nDigits;
  int decExponent;
  char[] digits;
  public FloatingDecimalWrapper() {
    try {
      isNegative = false;
      decExponent = 0;
      digits = new char[64];
      nDigits = 0;
      _floatingDecimal = CTOR.newInstance(isNegative, decExponent, digits, nDigits, false);
    } catch (Exception e) { throw Throwables.propagate(e); }
  }
  
  private void syncFloatingDecimal() throws Exception {
    IS_NEGATIVE.setBoolean(_floatingDecimal, isNegative);
    N_DIGITS.setInt(_floatingDecimal, nDigits);
    DEC_EXPONENT.setInt(_floatingDecimal, decExponent);
  }
  
  public double doubleValue() {
    try {
      syncFloatingDecimal();
      return (Double) DOUBLE_VALUE.invoke(_floatingDecimal);
    } catch (Exception e) { throw Throwables.propagate(e); }
  }
  
  public float floatValue() {
    try {
      syncFloatingDecimal();
      return (Float) FLOAT_VALUE.invoke(_floatingDecimal);
    } catch (Exception e) { throw Throwables.propagate(e); }
  }
}
