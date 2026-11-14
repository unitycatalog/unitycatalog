package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.Locale;
import java.util.Map;
import org.hibernate.Session;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.ConstraintViolationException.ConstraintKind;

/** Turns a unique-constraint violation, found by a check-then-write race, into a conflict. */
public final class UniqueViolations {

  private UniqueViolations() {}

  /**
   * Writes the pending changes now, so a violation fails here and not at commit. A unique violation
   * of a constraint in {@code messageByConstraint} (keyed by lower-case name) becomes {@link
   * ErrorCode#ALREADY_EXISTS} with that constraint's message; any other failure is rethrown.
   * Hibernate marks the violation as UNIQUE on H2 and MySQL but not on PostgreSQL, so SQLSTATE
   * 23505 (unique violation on PostgreSQL and H2) is checked as well.
   */
  public static void flushOrAlreadyExists(
      Session session, Map<String, String> messageByConstraint) {
    try {
      session.flush();
    } catch (ConstraintViolationException e) {
      boolean isUnique = e.getKind() == ConstraintKind.UNIQUE || "23505".equals(e.getSQLState());
      String name = e.getConstraintName();
      if (isUnique && name != null) {
        String lowerName = name.toLowerCase(Locale.ROOT);
        for (var constraint : messageByConstraint.entrySet()) {
          if (lowerName.contains(constraint.getKey())) {
            throw new BaseException(ErrorCode.ALREADY_EXISTS, constraint.getValue(), e);
          }
        }
      }
      throw e;
    }
  }
}
