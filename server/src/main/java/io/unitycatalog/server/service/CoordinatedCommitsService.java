package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.KeyMapperUtil;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.CommitRepository;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.IdentityUtils;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.unitycatalog.server.handler.CoordinatedCommitsHandler.*;
import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.TABLE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CoordinatedCommitsService {
  public static final CommitRepository COMMIT_REPOSITORY = CommitRepository.getInstance();
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private final UnityAccessEvaluator evaluator;

  @SneakyThrows
  public CoordinatedCommitsService(UnityCatalogAuthorizer authorizer) {
    evaluator = new UnityAccessEvaluator(authorizer);
  }

  @Post("")
  public HttpResponse commit(Commit commit) {
    authorizeForOperation(commit);
    validateCommit(commit);
    validateCommitTable(commit);

    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        List<CommitDAO> firstAndLastCommits =
            COMMIT_REPOSITORY.getFirstAndLastCommits(session, UUID.fromString(commit.getTableId()));
        if (firstAndLastCommits.isEmpty()) {
          handleFirstCommit(session, commit);
        } else {
          if (commit.getCommitInfo() == null) {
            handleBackfillOnlyCommit(
                session,
                commit.getTableId(),
                commit.getLatestBackfilledVersion(),
                firstAndLastCommits.get(0),
                firstAndLastCommits.get(1));
          } else {
            if (firstAndLastCommits.get(1).getIsDisownCommit()) {
              // This should never happen since disown commits are not allowed
              handleReboardCommit();
            } else {
              handleNormalCommit(
                  session, commit, firstAndLastCommits.get(0), firstAndLastCommits.get(1));
            }
          }
        }
        if (commit.getMetadata() != null) {
          COMMIT_REPOSITORY.updateTableMetadata(session, commit);
        }
        tx.commit();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
    return HttpResponse.of(HttpStatus.OK);
  }


  private void authorizeForOperation(Commit commit) {
    String expression = """
          #authorize(#principal, #metastore, OWNER) ||
          #authorize(#principal, #catalog, OWNER) ||
          (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
          (#authorize(#principal, #schema, USE_SCHEMA) && #authorize(#principal, #catalog, USE_CATALOG) && #authorizeAny(#principal, #table, OWNER, EXECUTE))
          """;

    Map<SecurableType, Object> resourceKeys = KeyMapperUtil.mapResourceKeys(
            Map.of(METASTORE, "metastore",
                    TABLE, commit.getTableId()));

    if (!evaluator.evaluate(IdentityUtils.findPrincipalId(), expression, resourceKeys)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
  }
}
