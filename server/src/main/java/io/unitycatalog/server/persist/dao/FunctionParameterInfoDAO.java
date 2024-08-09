package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.*;
import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.*;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_function_params")
// Lombok annotations
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class FunctionParameterInfoDAO extends IdentifiableDAO {
  public enum InputOrReturnEnum {
    INPUT,
    RETURN
  }

  @ManyToOne
  @JoinColumn(name = "function_id", referencedColumnName = "id")
  private FunctionInfoDAO function;

  // Whether the parameter is an input or return parameter
  @Column(name = "input_or_return", nullable = false)
  private InputOrReturnEnum inputOrReturn;

  @Column(name = "type_text", nullable = false)
  private String typeText;

  @Column(name = "type_json")
  private String typeJson;

  @Column(name = "type_name", nullable = false)
  private ColumnTypeName typeName;

  @Column(name = "type_precision")
  private Integer typePrecision;

  @Column(name = "type_scale")
  private Integer typeScale;

  @Column(name = "type_interval_type")
  private String typeIntervalType;

  @Column(name = "position")
  private Integer position;

  @Column(name = "parameter_mode")
  private FunctionParameterMode parameterMode;

  @Column(name = "parameter_type")
  private FunctionParameterType parameterType;

  @Column(name = "parameter_default")
  private String parameterDefault;

  @Column(name = "comment")
  private String comment;

  public static FunctionParameterInfoDAO from(
      FunctionParameterInfo functionParameterInfo, InputOrReturnEnum inputOrReturn) {
    return FunctionParameterInfoDAO.builder()
        .name(functionParameterInfo.getName())
        .typeText(functionParameterInfo.getTypeText())
        .typeJson(functionParameterInfo.getTypeJson())
        .typeName(functionParameterInfo.getTypeName())
        .typePrecision(functionParameterInfo.getTypePrecision())
        .typeScale(functionParameterInfo.getTypeScale())
        .typeIntervalType(functionParameterInfo.getTypeIntervalType())
        .position(functionParameterInfo.getPosition())
        .parameterMode(functionParameterInfo.getParameterMode())
        .parameterType(functionParameterInfo.getParameterType())
        .parameterDefault(functionParameterInfo.getParameterDefault())
        .inputOrReturn(inputOrReturn)
        .comment(functionParameterInfo.getComment())
        .build();
  }

  public FunctionParameterInfo toFunctionParameterInfo() {
    return new FunctionParameterInfo()
        .name(getName())
        .typeText(typeText)
        .typeJson(typeJson)
        .typeName(typeName)
        .typePrecision(typePrecision)
        .typeScale(typeScale)
        .typeIntervalType(typeIntervalType)
        .position(position)
        .parameterMode(parameterMode)
        .parameterType(parameterType)
        .parameterDefault(parameterDefault)
        .comment(comment);
  }

  public static List<FunctionParameterInfoDAO> from(
      FunctionParameterInfos functionParameterInfos, InputOrReturnEnum inputOrReturn) {
    if (functionParameterInfos == null || functionParameterInfos.getParameters() == null) {
      return new ArrayList<>();
    }
    return functionParameterInfos.getParameters().stream()
        .map(functionParameterInfo -> from(functionParameterInfo, inputOrReturn))
        .collect(Collectors.toList());
  }

  public static FunctionParameterInfos toFunctionParameterInfos(
      List<FunctionParameterInfoDAO> functionParameterInfoDAOs) {
    if (functionParameterInfoDAOs == null) return null;
    return new FunctionParameterInfos()
        .parameters(
            functionParameterInfoDAOs.stream()
                .map(FunctionParameterInfoDAO::toFunctionParameterInfo)
                .collect(Collectors.toList()));
  }
}
