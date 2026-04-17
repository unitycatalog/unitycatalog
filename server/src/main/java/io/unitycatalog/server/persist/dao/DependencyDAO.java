package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.Dependency;
import io.unitycatalog.server.model.FunctionDependency;
import io.unitycatalog.server.model.TableDependency;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.UuidGenerator;

@Entity
@Table(
    name = "uc_dependencies",
    indexes = {
      @Index(name = "idx_dependent", columnList = "dependent_type,dependent_id"),
    })
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class DependencyDAO {

  /**
   * Type of the object that owns dependencies (the "dependent"). Currently only tables (including
   * metric views) can own dependencies.
   */
  public enum DependentType {
    TABLE
  }

  /** Type of the dependency target. Mirrors the {@code Dependency} API model. */
  public enum DependencyType {
    TABLE,
    FUNCTION
  }

  @Id
  @UuidGenerator
  @Column(name = "id", updatable = false, nullable = false)
  private UUID id;

  @Enumerated(EnumType.STRING)
  @Column(name = "dependent_type", nullable = false)
  private DependentType dependentType;

  @Column(name = "dependent_id", nullable = false)
  private UUID dependentId;

  @Enumerated(EnumType.STRING)
  @Column(name = "dependency_type", nullable = false)
  private DependencyType dependencyType;

  @Column(name = "dependency_catalog")
  private String dependencyCatalog;

  @Column(name = "dependency_schema")
  private String dependencySchema;

  @Column(name = "dependency_name")
  private String dependencyName;

  /** Converts a Dependency API model to a DependencyDAO for a given dependent. */
  public static DependencyDAO from(
      Dependency dependency, UUID dependentId, DependentType dependentType) {
    DependencyDAOBuilder builder =
        DependencyDAO.builder().dependentId(dependentId).dependentType(dependentType);

    if (dependency.getTable() != null) {
      builder.dependencyType(DependencyType.TABLE);
      populateThreePartName(
          builder, dependency.getTable().getTableFullName(), DependencyType.TABLE);
    } else if (dependency.getFunction() != null) {
      builder.dependencyType(DependencyType.FUNCTION);
      populateThreePartName(
          builder, dependency.getFunction().getFunctionFullName(), DependencyType.FUNCTION);
    } else {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Unsupported dependency type");
    }

    return builder.build();
  }

  /**
   * Splits a {@code catalog.schema.name} three-part name into the corresponding DAO columns. Throws
   * if the input is null/empty or does not contain exactly three parts.
   */
  private static void populateThreePartName(
      DependencyDAOBuilder builder, String fullName, DependencyType type) {
    String kind = type.name().toLowerCase(Locale.ROOT);
    if (fullName == null || fullName.isEmpty()) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Dependency " + kind + " full name must not be null or empty");
    }
    String[] parts = fullName.split("\\.");
    if (parts.length != 3) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Dependency "
              + kind
              + " full name must be a three-part name (catalog.schema."
              + kind
              + "); got: "
              + fullName);
    }
    builder.dependencyCatalog(parts[0]);
    builder.dependencySchema(parts[1]);
    builder.dependencyName(parts[2]);
  }

  /** Converts this DAO to a Dependency API model. */
  public Dependency toDependency() {
    Dependency dependency = new Dependency();
    String fullName = dependencyCatalog + "." + dependencySchema + "." + dependencyName;
    switch (dependencyType) {
      case TABLE:
        dependency.setTable(new TableDependency().tableFullName(fullName));
        break;
      case FUNCTION:
        dependency.setFunction(new FunctionDependency().functionFullName(fullName));
        break;
    }
    return dependency;
  }

  public static List<Dependency> toDependencyList(List<DependencyDAO> daos) {
    if (daos == null) {
      return new ArrayList<>();
    }
    return daos.stream().map(DependencyDAO::toDependency).collect(Collectors.toList());
  }
}
