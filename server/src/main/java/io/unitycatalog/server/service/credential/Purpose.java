package io.unitycatalog.server.service.credential;

/** Who a vended storage credential is for. */
public enum Purpose {
  /** Returned to the API caller. */
  CLIENT,
  /** Used by the server itself while handling a request. */
  SERVER,
  /** Used by the background storage cleanup. */
  CLEANUP
}
