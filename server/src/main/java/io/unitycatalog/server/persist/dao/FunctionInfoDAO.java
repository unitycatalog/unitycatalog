package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.FunctionInfo;
import jakarta.persistence.*;
import java.util.List;
import java.util.UUID;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.hibernate.annotations.SQLRestriction;

// Hibernate annotations
@Entity
@Table(name = "uc_functions")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class FunctionInfoDAO extends IdentifiableDAO {
  @Column(name = "schema_id")
  private UUID schemaId;

  @Column(name = "comment", length = 65535)
  private String comment;

  @Column(name = "owner")
  private String owner;

  @Column(name = "created_at")
  private Long createdAt;

  @Column(name = "created_by")
  private String createdBy;

  @Column(name = "updated_at")
  private Long updatedAt;

  @Column(name = "updated_by")
  private String updatedBy;

  @Column(name = "data_type")
  private ColumnTypeName dataType;

  @Column(name = "full_data_type")
  private String fullDataType;

  @Column(name = "external_language")
  private String externalLanguage;

  @Column(name = "is_deterministic")
  private Boolean isDeterministic;

  @Column(name = "is_null_call")
  private Boolean isNullCall;

  @Column(name = "parameter_style")
  private FunctionInfo.ParameterStyleEnum parameterStyle;

  @Column(name = "routine_body")
  private FunctionInfo.RoutineBodyEnum routineBody;

  @Lob
  @Column(name = "routine_definition", length = 16777215)
  private String routineDefinition;

  @Column(name = "sql_data_access")
  private FunctionInfo.SqlDataAccessEnum sqlDataAccess;

  @Column(name = "security_type")
  private FunctionInfo.SecurityTypeEnum securityType;

  @Column(name = "specific_name")
  private String specificName;

  @OneToMany(mappedBy = "function", cascade = CascadeType.ALL, orphanRemoval = true)
  @SQLRestriction("input_or_return = 0")
  private List<FunctionParameterInfoDAO> inputParams;

  @OneToMany(mappedBy = "function", cascade = CascadeType.ALL, orphanRemoval = true)
  @SQLRestriction("input_or_return = 1")
  private List<FunctionParameterInfoDAO> returnParams;

  public static FunctionInfoDAO from(FunctionInfo functionInfo) {
    FunctionInfoDAO functionInfoDAO =
        FunctionInfoDAO.builder()
            .id(
                functionInfo.getFunctionId() != null
                    ? UUID.fromString(functionInfo.getFunctionId())
                    : null)
            .name(functionInfo.getName())
            .comment(functionInfo.getComment())
            .owner(functionInfo.getOwner())
            .createdAt(functionInfo.getCreatedAt())
            .createdBy(functionInfo.getCreatedBy())
            .updatedAt(functionInfo.getUpdatedAt())
            .updatedBy(functionInfo.getUpdatedBy())
            .dataType(functionInfo.getDataType())
            .fullDataType(functionInfo.getFullDataType())
            .externalLanguage(functionInfo.getExternalLanguage())
            .isDeterministic(functionInfo.getIsDeterministic())
            .isNullCall(functionInfo.getIsNullCall())
            .parameterStyle(functionInfo.getParameterStyle())
            .routineBody(functionInfo.getRoutineBody())
            .routineDefinition(functionInfo.getRoutineDefinition())
            .sqlDataAccess(functionInfo.getSqlDataAccess())
            .securityType(functionInfo.getSecurityType())
            .specificName(functionInfo.getSpecificName())
            .inputParams(
                FunctionParameterInfoDAO.from(
                    functionInfo.getInputParams(),
                    FunctionParameterInfoDAO.InputOrReturnEnum.INPUT))
            .returnParams(
                FunctionParameterInfoDAO.from(
                    functionInfo.getReturnParams(),
                    FunctionParameterInfoDAO.InputOrReturnEnum.RETURN))
            .build();
    for (FunctionParameterInfoDAO inputParam : functionInfoDAO.inputParams) {
      inputParam.setFunction(functionInfoDAO);
    }
    for (FunctionParameterInfoDAO returnParam : functionInfoDAO.returnParams) {
      returnParam.setFunction(functionInfoDAO);
    }
    return functionInfoDAO;
  }

  public FunctionInfo toFunctionInfo() {
    FunctionInfo functionInfo =
        new FunctionInfo()
            .functionId(getId().toString())
            .name(getName())
            .comment(comment)
            .owner(owner)
            .createdAt(createdAt)
            .createdBy(createdBy)
            .updatedAt(updatedAt)
            .updatedBy(updatedBy)
            .dataType(dataType)
            .fullDataType(fullDataType)
            .externalLanguage(externalLanguage)
            .isDeterministic(isDeterministic)
            .isNullCall(isNullCall)
            .parameterStyle(parameterStyle)
            .routineBody(routineBody)
            .routineDefinition(routineDefinition)
            .sqlDataAccess(sqlDataAccess)
            .securityType(securityType)
            .specificName(specificName);
    if (!inputParams.isEmpty()) {
      functionInfo.inputParams(FunctionParameterInfoDAO.toFunctionParameterInfos(inputParams));
    }
    if (!returnParams.isEmpty()) {
      functionInfo.returnParams(FunctionParameterInfoDAO.toFunctionParameterInfos(returnParams));
    }
    return functionInfo;
  }
}
