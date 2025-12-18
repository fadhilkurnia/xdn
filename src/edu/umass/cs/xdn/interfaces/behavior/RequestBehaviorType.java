package edu.umass.cs.xdn.interfaces.behavior;

public enum RequestBehaviorType {
  READ_ONLY,
  WRITE_ONLY,
  READ_MODIFY_WRITE,
  NIL_EXTERNAL,
  MONOTONIC,
  COMMUTATIVE;

  public static RequestBehaviorType fromString(String behavior) throws IllegalArgumentException {
    if (behavior == null) {
      throw new IllegalArgumentException("behavior cannot be null");
    }

    behavior = behavior.trim();
    behavior = behavior.toUpperCase();

    // handle alias of READ_MODIFY_WRITE: READ_WRITE
    if (behavior.equals("READ_WRITE")) {
      behavior = READ_MODIFY_WRITE.toString();
    }

    return RequestBehaviorType.valueOf(behavior);
  }
}
