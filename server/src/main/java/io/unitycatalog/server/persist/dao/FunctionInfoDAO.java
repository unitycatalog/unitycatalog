package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.FunctionInfo;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.SQLRestriction;
import io.unitycatalog.server.model.ColumnTypeName;

import java.util.List;

// Hibernate annotations
@Entity
@Table(name = "uc_functions")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
public class FunctionInfoDAO {
    @Id
    @Column(name = "function_id")
    private String functionId;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "catalog_name", nullable = false)
    private String catalogName;

    @Column(name = "schema_name", nullable = false)
    private String schemaName;

    @Column(name = "comment")
    private String comment;

    @Column(name = "created_at")
    private Long createdAt;

    @Column(name = "updated_at")
    private Long updatedAt;

    @Column(name = "data_type")
    private ColumnTypeName dataType;

    @Column(name = "full_name")
    private String fullName;

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

    @Column(name = "routine_definition")
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
        FunctionInfoDAO functionInfoDAO =  FunctionInfoDAO.builder()
                .functionId(functionInfo.getFunctionId())
                .name(functionInfo.getName())
                .catalogName(functionInfo.getCatalogName())
                .schemaName(functionInfo.getSchemaName())
                .comment(functionInfo.getComment())
                .createdAt(functionInfo.getCreatedAt())
                .updatedAt(functionInfo.getUpdatedAt())
                .dataType(functionInfo.getDataType())
                .fullName(functionInfo.getFullName())
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
                .inputParams(FunctionParameterInfoDAO.from(functionInfo.getInputParams(),
                        FunctionParameterInfoDAO.InputOrReturnEnum.INPUT))
                .returnParams(FunctionParameterInfoDAO.from(functionInfo.getReturnParams(),
                        FunctionParameterInfoDAO.InputOrReturnEnum.RETURN)).build();
        for (FunctionParameterInfoDAO inputParam : functionInfoDAO.inputParams) {
            inputParam.setFunction(functionInfoDAO);
        }
        for (FunctionParameterInfoDAO returnParam : functionInfoDAO.returnParams) {
            returnParam.setFunction(functionInfoDAO);
        }
        return functionInfoDAO;
    }

    public FunctionInfo toFunctionInfo() {
        FunctionInfo functionInfo = new FunctionInfo()
                .functionId(functionId)
                .name(name)
                .catalogName(catalogName)
                .schemaName(schemaName)
                .comment(comment)
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .dataType(dataType)
                .fullName(fullName)
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
        if (!inputParams.isEmpty())
            functionInfo.inputParams(FunctionParameterInfoDAO.toFunctionParameterInfos(inputParams));
        if (!returnParams.isEmpty())
            functionInfo.returnParams(FunctionParameterInfoDAO.toFunctionParameterInfos(returnParams));
        return functionInfo;
    }
}
