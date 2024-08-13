package io.unitycatalog.server.exception;

import com.unboundid.scim2.common.exceptions.ScimException;

public class Scim2RuntimeException extends RuntimeException {

  public Scim2RuntimeException(ScimException e) {
    super(e);
  }
}
