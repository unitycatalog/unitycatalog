package io.unitycatalog.server.auth.decorator;

/**
 * Computes an authorization value from an already-bound request body, for cases where a plain field
 * lookup cannot express it (e.g. the value depends on the contents of a list). Named on {@link
 * io.unitycatalog.server.auth.annotation.AuthorizeKey} or {@link
 * io.unitycatalog.server.auth.annotation.AuthorizeResourceKey} in place of a {@code key}, and
 * invoked by {@link AuthorizeKeyLocator}.
 *
 * <p>Implementations must be stateless and expose a public no-arg constructor; one instance is
 * created per annotation.
 *
 * <p>The return type depends on which annotation the extractor is on:
 *
 * <ul>
 *   <li>On {@code @AuthorizeResourceKey}, the value is mapped to a resource id by {@code
 *       KeyMapper}, so it must be a {@code String} (a name, full name, or storage path), a {@code
 *       UUID}, or {@code null} ({@code null} means "not set" and is skipped).
 *   <li>On {@code @AuthorizeKey}, the value is exposed directly as a SpEL variable, so its type
 *       only has to match how the expression uses it (e.g. a {@code Boolean} for a ternary
 *       condition, a {@code String} for an {@code == '...'} comparison). Enums are surfaced as
 *       their {@code toString()}; any other type is passed through as-is.
 * </ul>
 */
public interface AuthorizeValueExtractor {

  /**
   * @param body the bound request body (the exact object the handler receives); never null.
   * @return the value to expose (a resource key to map, or a raw SpEL variable), or null. See the
   *     type contract on the interface.
   */
  Object extract(Object body);
}
