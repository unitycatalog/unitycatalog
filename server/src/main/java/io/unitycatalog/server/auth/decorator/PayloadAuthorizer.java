package io.unitycatalog.server.auth.decorator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;

/**
 * Per-request PAYLOAD-source authorization callback. {@link UnityAccessDecorator} stashes one in
 * the request context under {@link UnityAccessDecorator#PAYLOAD_AUTHORIZER} for every request it
 * handles; {@link AuthorizationGateConverter} invokes it on the bound body object right after
 * binding and before the handler runs, so authorization sees the exact value the handler will
 * receive.
 */
@FunctionalInterface
public interface PayloadAuthorizer {

  /**
   * No body-derived check is required: the method declares no PAYLOAD key and was authorized
   * inline. Set explicitly so the gate receives that as a value instead of inferring it from a
   * missing one.
   */
  PayloadAuthorizer NO_BODY_CHECK_REQUIRED = (body, mapper) -> {};

  /**
   * Authorizes the request against the already-bound body.
   *
   * @param body the object the delegate converter bound the request body to (may be {@code null},
   *     e.g. a JSON literal {@code null} body)
   * @param mapper the same mapper used to bind {@code body}, used to re-serialize it to a
   *     wire-name-keyed map so locator keys resolve the way the handler's binding did
   * @throws BaseException with {@link ErrorCode#PERMISSION_DENIED} if the request is not authorized
   */
  void authorize(Object body, ObjectMapper mapper);
}
