package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_ICEBERG_PLAN_VALUE;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_ICEBERG_CREDENTIALS_ENDPOINT_KEY;
import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_ICEBERG_PLAN_ID_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CredId} keyed by credential context, Iceberg credentials endpoint, and scan plan ID.
 *
 * <p>The plan ID identifies the authorized server-side scan, rather than the source table. This
 * keeps renewed credentials scoped to the same FGAC plan as the credentials returned with the
 * original plan response.
 */
public final class IcebergPlanCredId implements CredId {
  private final String credContextId;
  private final String credentialsEndpoint;
  private final String planId;

  public IcebergPlanCredId(String credContextId, String credentialsEndpoint, String planId) {
    Preconditions.checkNotNull(credContextId, "credContextId is required");
    Preconditions.checkArgument(
        credentialsEndpoint != null && !credentialsEndpoint.isEmpty(),
        "credentialsEndpoint is required");
    Preconditions.checkArgument(planId != null && !planId.isEmpty(), "planId is required");
    this.credContextId = credContextId;
    this.credentialsEndpoint = credentialsEndpoint;
    this.planId = planId;
  }

  public String credentialsEndpoint() {
    return credentialsEndpoint;
  }

  public String planId() {
    return planId;
  }

  @Override
  public Map<String, String> props() {
    return Map.of(
        UC_CRED_CONTEXT_ID_KEY,
        credContextId,
        UC_CREDENTIALS_TYPE_KEY,
        UC_CREDENTIALS_TYPE_ICEBERG_PLAN_VALUE,
        UC_ICEBERG_CREDENTIALS_ENDPOINT_KEY,
        credentialsEndpoint,
        UC_ICEBERG_PLAN_ID_KEY,
        planId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof IcebergPlanCredId)) return false;
    IcebergPlanCredId that = (IcebergPlanCredId) o;
    return Objects.equals(credContextId, that.credContextId)
        && Objects.equals(credentialsEndpoint, that.credentialsEndpoint)
        && Objects.equals(planId, that.planId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credContextId, credentialsEndpoint, planId);
  }

  @Override
  public String toString() {
    return "IcebergPlanCredId{credContextId="
        + credContextId
        + ", credentialsEndpoint="
        + credentialsEndpoint
        + ", planId="
        + planId
        + "}";
  }
}
