/*
 * Unity Catalog API
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.1
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.unitycatalog.client.model;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.FunctionParameterMode;
import io.unitycatalog.client.model.FunctionParameterType;
import java.util.Arrays;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * FunctionParameterInfo
 */
@JsonPropertyOrder({
  FunctionParameterInfo.JSON_PROPERTY_NAME,
  FunctionParameterInfo.JSON_PROPERTY_TYPE_TEXT,
  FunctionParameterInfo.JSON_PROPERTY_TYPE_JSON,
  FunctionParameterInfo.JSON_PROPERTY_TYPE_NAME,
  FunctionParameterInfo.JSON_PROPERTY_TYPE_PRECISION,
  FunctionParameterInfo.JSON_PROPERTY_TYPE_SCALE,
  FunctionParameterInfo.JSON_PROPERTY_TYPE_INTERVAL_TYPE,
  FunctionParameterInfo.JSON_PROPERTY_POSITION,
  FunctionParameterInfo.JSON_PROPERTY_PARAMETER_MODE,
  FunctionParameterInfo.JSON_PROPERTY_PARAMETER_TYPE,
  FunctionParameterInfo.JSON_PROPERTY_PARAMETER_DEFAULT,
  FunctionParameterInfo.JSON_PROPERTY_COMMENT
})
@jakarta.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.5.0")
public class FunctionParameterInfo {
  public static final String JSON_PROPERTY_NAME = "name";
  private String name;

  public static final String JSON_PROPERTY_TYPE_TEXT = "type_text";
  private String typeText;

  public static final String JSON_PROPERTY_TYPE_JSON = "type_json";
  private String typeJson;

  public static final String JSON_PROPERTY_TYPE_NAME = "type_name";
  private ColumnTypeName typeName;

  public static final String JSON_PROPERTY_TYPE_PRECISION = "type_precision";
  private Integer typePrecision;

  public static final String JSON_PROPERTY_TYPE_SCALE = "type_scale";
  private Integer typeScale;

  public static final String JSON_PROPERTY_TYPE_INTERVAL_TYPE = "type_interval_type";
  private String typeIntervalType;

  public static final String JSON_PROPERTY_POSITION = "position";
  private Integer position;

  public static final String JSON_PROPERTY_PARAMETER_MODE = "parameter_mode";
  private FunctionParameterMode parameterMode;

  public static final String JSON_PROPERTY_PARAMETER_TYPE = "parameter_type";
  private FunctionParameterType parameterType;

  public static final String JSON_PROPERTY_PARAMETER_DEFAULT = "parameter_default";
  private String parameterDefault;

  public static final String JSON_PROPERTY_COMMENT = "comment";
  private String comment;

  public FunctionParameterInfo() { 
  }

  public FunctionParameterInfo name(String name) {
    this.name = name;
    return this;
  }

   /**
   * Name of parameter.
   * @return name
  **/
  @jakarta.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_NAME)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getName() {
    return name;
  }


  @JsonProperty(JSON_PROPERTY_NAME)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setName(String name) {
    this.name = name;
  }


  public FunctionParameterInfo typeText(String typeText) {
    this.typeText = typeText;
    return this;
  }

   /**
   * Full data type spec, SQL/catalogString text.
   * @return typeText
  **/
  @jakarta.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_TYPE_TEXT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getTypeText() {
    return typeText;
  }


  @JsonProperty(JSON_PROPERTY_TYPE_TEXT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setTypeText(String typeText) {
    this.typeText = typeText;
  }


  public FunctionParameterInfo typeJson(String typeJson) {
    this.typeJson = typeJson;
    return this;
  }

   /**
   * Full data type spec, JSON-serialized.
   * @return typeJson
  **/
  @jakarta.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_TYPE_JSON)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getTypeJson() {
    return typeJson;
  }


  @JsonProperty(JSON_PROPERTY_TYPE_JSON)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setTypeJson(String typeJson) {
    this.typeJson = typeJson;
  }


  public FunctionParameterInfo typeName(ColumnTypeName typeName) {
    this.typeName = typeName;
    return this;
  }

   /**
   * Get typeName
   * @return typeName
  **/
  @jakarta.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_TYPE_NAME)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public ColumnTypeName getTypeName() {
    return typeName;
  }


  @JsonProperty(JSON_PROPERTY_TYPE_NAME)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setTypeName(ColumnTypeName typeName) {
    this.typeName = typeName;
  }


  public FunctionParameterInfo typePrecision(Integer typePrecision) {
    this.typePrecision = typePrecision;
    return this;
  }

   /**
   * Digits of precision; required on Create for DecimalTypes.
   * @return typePrecision
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_TYPE_PRECISION)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Integer getTypePrecision() {
    return typePrecision;
  }


  @JsonProperty(JSON_PROPERTY_TYPE_PRECISION)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setTypePrecision(Integer typePrecision) {
    this.typePrecision = typePrecision;
  }


  public FunctionParameterInfo typeScale(Integer typeScale) {
    this.typeScale = typeScale;
    return this;
  }

   /**
   * Digits to right of decimal; Required on Create for DecimalTypes.
   * @return typeScale
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_TYPE_SCALE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Integer getTypeScale() {
    return typeScale;
  }


  @JsonProperty(JSON_PROPERTY_TYPE_SCALE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setTypeScale(Integer typeScale) {
    this.typeScale = typeScale;
  }


  public FunctionParameterInfo typeIntervalType(String typeIntervalType) {
    this.typeIntervalType = typeIntervalType;
    return this;
  }

   /**
   * Format of IntervalType.
   * @return typeIntervalType
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_TYPE_INTERVAL_TYPE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getTypeIntervalType() {
    return typeIntervalType;
  }


  @JsonProperty(JSON_PROPERTY_TYPE_INTERVAL_TYPE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setTypeIntervalType(String typeIntervalType) {
    this.typeIntervalType = typeIntervalType;
  }


  public FunctionParameterInfo position(Integer position) {
    this.position = position;
    return this;
  }

   /**
   * Ordinal position of column (starting at position 0).
   * @return position
  **/
  @jakarta.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_POSITION)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getPosition() {
    return position;
  }


  @JsonProperty(JSON_PROPERTY_POSITION)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setPosition(Integer position) {
    this.position = position;
  }


  public FunctionParameterInfo parameterMode(FunctionParameterMode parameterMode) {
    this.parameterMode = parameterMode;
    return this;
  }

   /**
   * Get parameterMode
   * @return parameterMode
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_PARAMETER_MODE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public FunctionParameterMode getParameterMode() {
    return parameterMode;
  }


  @JsonProperty(JSON_PROPERTY_PARAMETER_MODE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setParameterMode(FunctionParameterMode parameterMode) {
    this.parameterMode = parameterMode;
  }


  public FunctionParameterInfo parameterType(FunctionParameterType parameterType) {
    this.parameterType = parameterType;
    return this;
  }

   /**
   * Get parameterType
   * @return parameterType
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_PARAMETER_TYPE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public FunctionParameterType getParameterType() {
    return parameterType;
  }


  @JsonProperty(JSON_PROPERTY_PARAMETER_TYPE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setParameterType(FunctionParameterType parameterType) {
    this.parameterType = parameterType;
  }


  public FunctionParameterInfo parameterDefault(String parameterDefault) {
    this.parameterDefault = parameterDefault;
    return this;
  }

   /**
   * Default value of the parameter.
   * @return parameterDefault
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_PARAMETER_DEFAULT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getParameterDefault() {
    return parameterDefault;
  }


  @JsonProperty(JSON_PROPERTY_PARAMETER_DEFAULT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setParameterDefault(String parameterDefault) {
    this.parameterDefault = parameterDefault;
  }


  public FunctionParameterInfo comment(String comment) {
    this.comment = comment;
    return this;
  }

   /**
   * User-provided free-form text description.
   * @return comment
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_COMMENT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getComment() {
    return comment;
  }


  @JsonProperty(JSON_PROPERTY_COMMENT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setComment(String comment) {
    this.comment = comment;
  }


  /**
   * Return true if this FunctionParameterInfo object is equal to o.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionParameterInfo functionParameterInfo = (FunctionParameterInfo) o;
    return Objects.equals(this.name, functionParameterInfo.name) &&
        Objects.equals(this.typeText, functionParameterInfo.typeText) &&
        Objects.equals(this.typeJson, functionParameterInfo.typeJson) &&
        Objects.equals(this.typeName, functionParameterInfo.typeName) &&
        Objects.equals(this.typePrecision, functionParameterInfo.typePrecision) &&
        Objects.equals(this.typeScale, functionParameterInfo.typeScale) &&
        Objects.equals(this.typeIntervalType, functionParameterInfo.typeIntervalType) &&
        Objects.equals(this.position, functionParameterInfo.position) &&
        Objects.equals(this.parameterMode, functionParameterInfo.parameterMode) &&
        Objects.equals(this.parameterType, functionParameterInfo.parameterType) &&
        Objects.equals(this.parameterDefault, functionParameterInfo.parameterDefault) &&
        Objects.equals(this.comment, functionParameterInfo.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, typeText, typeJson, typeName, typePrecision, typeScale, typeIntervalType, position, parameterMode, parameterType, parameterDefault, comment);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FunctionParameterInfo {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    typeText: ").append(toIndentedString(typeText)).append("\n");
    sb.append("    typeJson: ").append(toIndentedString(typeJson)).append("\n");
    sb.append("    typeName: ").append(toIndentedString(typeName)).append("\n");
    sb.append("    typePrecision: ").append(toIndentedString(typePrecision)).append("\n");
    sb.append("    typeScale: ").append(toIndentedString(typeScale)).append("\n");
    sb.append("    typeIntervalType: ").append(toIndentedString(typeIntervalType)).append("\n");
    sb.append("    position: ").append(toIndentedString(position)).append("\n");
    sb.append("    parameterMode: ").append(toIndentedString(parameterMode)).append("\n");
    sb.append("    parameterType: ").append(toIndentedString(parameterType)).append("\n");
    sb.append("    parameterDefault: ").append(toIndentedString(parameterDefault)).append("\n");
    sb.append("    comment: ").append(toIndentedString(comment)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  /**
   * Convert the instance into URL query string.
   *
   * @return URL query string
   */
  public String toUrlQueryString() {
    return toUrlQueryString(null);
  }

  /**
   * Convert the instance into URL query string.
   *
   * @param prefix prefix of the query string
   * @return URL query string
   */
  public String toUrlQueryString(String prefix) {
    String suffix = "";
    String containerSuffix = "";
    String containerPrefix = "";
    if (prefix == null) {
      // style=form, explode=true, e.g. /pet?name=cat&type=manx
      prefix = "";
    } else {
      // deepObject style e.g. /pet?id[name]=cat&id[type]=manx
      prefix = prefix + "[";
      suffix = "]";
      containerSuffix = "]";
      containerPrefix = "[";
    }

    StringJoiner joiner = new StringJoiner("&");

    // add `name` to the URL query string
    if (getName() != null) {
      joiner.add(String.format("%sname%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getName()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `type_text` to the URL query string
    if (getTypeText() != null) {
      joiner.add(String.format("%stype_text%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getTypeText()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `type_json` to the URL query string
    if (getTypeJson() != null) {
      joiner.add(String.format("%stype_json%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getTypeJson()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `type_name` to the URL query string
    if (getTypeName() != null) {
      joiner.add(String.format("%stype_name%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getTypeName()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `type_precision` to the URL query string
    if (getTypePrecision() != null) {
      joiner.add(String.format("%stype_precision%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getTypePrecision()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `type_scale` to the URL query string
    if (getTypeScale() != null) {
      joiner.add(String.format("%stype_scale%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getTypeScale()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `type_interval_type` to the URL query string
    if (getTypeIntervalType() != null) {
      joiner.add(String.format("%stype_interval_type%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getTypeIntervalType()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `position` to the URL query string
    if (getPosition() != null) {
      joiner.add(String.format("%sposition%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getPosition()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `parameter_mode` to the URL query string
    if (getParameterMode() != null) {
      joiner.add(String.format("%sparameter_mode%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getParameterMode()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `parameter_type` to the URL query string
    if (getParameterType() != null) {
      joiner.add(String.format("%sparameter_type%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getParameterType()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `parameter_default` to the URL query string
    if (getParameterDefault() != null) {
      joiner.add(String.format("%sparameter_default%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getParameterDefault()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    // add `comment` to the URL query string
    if (getComment() != null) {
      joiner.add(String.format("%scomment%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getComment()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    return joiner.toString();
  }
}

