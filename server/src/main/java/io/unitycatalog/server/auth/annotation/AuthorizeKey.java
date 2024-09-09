package io.unitycatalog.server.auth.annotation;

import io.unitycatalog.server.model.ResourceType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Map a request parameter to a unity catalog resource key.
 *
 * <p>This annotation is used to map a request parameter to a unity catalog resource key. The
 * resource key is used to retrieve the resource identifier, which is then used to authorize the
 * request. As an example, you are making a request the retrieve a schema, the parameter that
 * contains the schema name might be defined in the request as - @Param("full_Name") String fullName
 * - This annotation would take the value of the fullName parameter and use it to retrieve the
 * schema resource identifier.
 *
 * <p>This annotation can be used multiple times per service method. The interpretation of the
 * annotation changes depending on how it is used.
 *
 * <p>Method level - When used at the method level, it configures a server-level attribute for the
 * request. Currently, the only server level attribute is the METASTORE. When used at the method
 * level, it is expected that the key is left unset.
 *
 * <p>Example: @AuthorizeKey(METASTORE) public void serviceMethod(...) { }
 *
 * <p>Method parameter level with Armeria @Param annotation - When used on a method parameter, and
 * the parameter also has an Armeria @Param annotation defined, the key is taken from the @Param
 * annotations value and that is what is used to retrieve the resource value. Since the key is taken
 * from the @Param, the key value in this annotation should be left unset.
 *
 * <p>Example: public void serviceMethod(@Param("catalog") @AuthorizeKey(CATALOG) String catalog) {
 * }
 *
 * <p>Method parameter level on payload parameter - When used on a method parameter, and there is no
 * corresponding Armeria @Param annotation, the annotation key field is required. That key is used
 * to retrieve the resource value from the request payload.
 *
 * <p>Example: public void serviceMethod(@AuthorizeKey(value = CATALOG, key = "catalog")
 * CreateSchemaRequest request) { }
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
public @interface AuthorizeKey {

  ResourceType value();

  String key() default "";
}