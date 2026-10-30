package io.unitycatalog.server.auth;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.IdentifiableDAO;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.JsonUtils;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

/** Builds OPA input from identity and resource metadata, without native grants or credentials. */
class OpaInputBuilder {
  private final SessionFactory sessionFactory;

  OpaInputBuilder(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  ObjectNode build(
      UUID principalId, SecurableType resourceType, UUID resourceId, Privileges privilege) {
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          UserDAO user = required(session, UserDAO.class, principalId);
          ObjectNode request = JsonUtils.getInstance().createObjectNode();
          ObjectNode input = request.putObject("input");
          ObjectNode principal =
              input
                  .putObject("principal")
                  .put("id", user.getId().toString())
                  .put("email", user.getEmail());
          if (user.getExternalId() != null) {
            principal.put("external_id", user.getExternalId());
          }
          input.put("privilege", privilege.name());
          input.set("resource", resource(session, resourceType, resourceId));
          return request;
        },
        "Could not build OPA authorization input",
        /* readOnly= */ true);
  }

  private ObjectNode resource(Session session, SecurableType type, UUID id) {
    return switch (type) {
      case METASTORE -> {
        required(session, MetastoreDAO.class, id);
        ObjectNode metastore =
            JsonUtils.getInstance()
                .createObjectNode()
                .put("id", id.toString())
                .put("type", type.name());
        metastore.putArray("parents");
        yield metastore;
      }
      case CATALOG -> withParents(type, required(session, CatalogInfoDAO.class, id));
      case SCHEMA -> {
        SchemaInfoDAO schema = required(session, SchemaInfoDAO.class, id);
        yield withParents(
            type,
            schema,
            describe(
                SecurableType.CATALOG,
                required(session, CatalogInfoDAO.class, schema.getCatalogId())));
      }
      case TABLE -> {
        TableInfoDAO table = session.get(TableInfoDAO.class, id);
        if (table != null) {
          yield dataResource(session, type, table, table.getSchemaId());
        }
        StagingTableDAO staging = required(session, StagingTableDAO.class, id);
        yield dataResource(session, type, staging, staging.getSchemaId());
      }
      case VOLUME -> {
        VolumeInfoDAO volume = required(session, VolumeInfoDAO.class, id);
        yield dataResource(session, type, volume, volume.getSchemaId());
      }
      case FUNCTION -> {
        FunctionInfoDAO function = required(session, FunctionInfoDAO.class, id);
        yield dataResource(session, type, function, function.getSchemaId());
      }
      case REGISTERED_MODEL -> {
        RegisteredModelInfoDAO model = required(session, RegisteredModelInfoDAO.class, id);
        yield dataResource(session, type, model, model.getSchemaId());
      }
      case EXTERNAL_LOCATION -> withParents(type, required(session, ExternalLocationDAO.class, id));
      case CREDENTIAL -> withParents(type, required(session, CredentialDAO.class, id));
    };
  }

  private ObjectNode dataResource(
      Session session, SecurableType type, IdentifiableDAO resource, UUID schemaId) {
    SchemaInfoDAO schema = required(session, SchemaInfoDAO.class, schemaId);
    CatalogInfoDAO catalog = required(session, CatalogInfoDAO.class, schema.getCatalogId());
    return withParents(
        type,
        resource,
        describe(SecurableType.SCHEMA, schema),
        describe(SecurableType.CATALOG, catalog));
  }

  private ObjectNode withParents(
      SecurableType type, IdentifiableDAO resource, ObjectNode... parents) {
    ObjectNode result = describe(type, resource);
    ArrayNode parentNodes = result.putArray("parents");
    for (ObjectNode parent : parents) {
      parentNodes.add(parent);
    }
    return result;
  }

  private ObjectNode describe(SecurableType type, IdentifiableDAO resource) {
    return JsonUtils.getInstance()
        .createObjectNode()
        .put("id", resource.getId().toString())
        .put("type", type.name())
        .put("name", resource.getName());
  }

  private <T> T required(Session session, Class<T> type, UUID id) {
    T value = session.get(type, id);
    if (value == null) {
      throw new BaseException(ErrorCode.UNAVAILABLE, "Could not resolve authorization metadata");
    }
    return value;
  }
}
