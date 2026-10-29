package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins each Delta Kernel schema-evolution behavior {@link DeltaSchemaEvolutionValidator} relies on,
 * so a Kernel upgrade that changes one fails here.
 */
public class DeltaSchemaEvolutionValidatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Map<String, String> NAME =
      Map.of("delta.columnMapping.mode", "name", "delta.columnMapping.maxColumnId", "5");
  private static final Map<String, String> ID =
      Map.of("delta.columnMapping.mode", "id", "delta.columnMapping.maxColumnId", "5");

  // s.x is clustered; t.x has the same physical name col-x under another parent.
  private static final Map<String, String> CLUSTERED_BY_S_X =
      Map.of(
          "delta.columnMapping.mode", "name",
          "delta.columnMapping.maxColumnId", "9",
          "clusteringColumns", "[[\"col-s\",\"col-x\"]]");

  private record Case(
      String name,
      Map<String, String> properties,
      boolean renamesTopLevelColumn,
      List<Map<String, Object>> current,
      List<Map<String, Object>> incoming,
      String rejection) {
    @Override
    public String toString() {
      return name;
    }
  }

  private static Case ok(
      String name,
      Map<String, String> props,
      boolean rename,
      List<Map<String, Object>> current,
      List<Map<String, Object>> incoming) {
    return new Case(name, props, rename, current, incoming, null);
  }

  private static Case rejected(
      String name,
      String rejection,
      Map<String, String> props,
      boolean rename,
      List<Map<String, Object>> current,
      List<Map<String, Object>> incoming) {
    return new Case(name, props, rename, current, incoming, rejection);
  }

  private static Map<String, Object> fld(String name, Object type, Integer id, String phys) {
    return fld(name, type, true, id, phys);
  }

  private static Map<String, Object> fld(
      String name, Object type, boolean nullable, Integer id, String phys) {
    Map<String, Object> meta = new LinkedHashMap<>();
    if (id != null) {
      meta.put("delta.columnMapping.id", id);
    }
    if (phys != null) {
      meta.put("delta.columnMapping.physicalName", phys);
    }
    Map<String, Object> field = new LinkedHashMap<>();
    field.put("name", name);
    field.put("type", type);
    field.put("nullable", nullable);
    field.put("metadata", meta);
    return field;
  }

  @SafeVarargs
  private static Map<String, Object> struct(Map<String, Object>... fields) {
    Map<String, Object> t = new LinkedHashMap<>();
    t.put("type", "struct");
    t.put("fields", List.of(fields));
    return t;
  }

  private static Map<String, Object> array(Object element) {
    Map<String, Object> t = new LinkedHashMap<>();
    t.put("type", "array");
    t.put("elementType", element);
    t.put("containsNull", true);
    return t;
  }

  private static Map<String, Object> map(Object key, Object value) {
    Map<String, Object> t = new LinkedHashMap<>();
    t.put("type", "map");
    t.put("keyType", key);
    t.put("valueType", value);
    t.put("valueContainsNull", true);
    return t;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> typeChange(Map<String, Object> field, String from, String to) {
    ((Map<String, Object>) field.get("metadata"))
        .put("delta.typeChanges", List.of(Map.of("fromType", from, "toType", to)));
    return field;
  }

  @SafeVarargs
  private static List<Map<String, Object>> cols(Map<String, Object>... fields) {
    return List.of(fields);
  }

  private static Map<String, String> with(Map<String, String> base, String key, String value) {
    Map<String, String> props = new HashMap<>(base);
    props.put(key, value);
    return props;
  }

  private static List<ColumnInfo> columns(List<Map<String, Object>> fields)
      throws JsonProcessingException {
    List<ColumnInfo> out = new ArrayList<>();
    for (Map<String, Object> field : fields) {
      out.add(
          new ColumnInfo()
              .name((String) field.get("name"))
              .typeJson(MAPPER.writeValueAsString(field)));
    }
    return out;
  }

  static Stream<Case> cases() {
    return Stream.of(
        // Type change
        rejected(
            "id mode rename and type change",
            "Cannot change the type of existing field",
            ID,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "string", 1, "col-1"))),
        // Tightened nullability
        rejected(
            "name mode rename and NOT NULL",
            "Cannot tighten the nullability",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", false, 1, "col-1"))),
        rejected(
            "id mode rename and NOT NULL",
            "Cannot tighten the nullability",
            ID,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", false, 1, "col-1"))),
        // Same id, new physical name
        rejected(
            "name mode rename and new physical name",
            "which is different from col-9",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-9"))),
        rejected(
            "id mode rename and new physical name",
            "which is different from col-9",
            ID,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-9"))),
        // New id at or below maxColumnId
        rejected(
            "name mode rename and reused id",
            "fieldId <= maxFieldId",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"), fld("c", "long", 3, "col-3"))),
        rejected(
            "id mode rename and reused id",
            "fieldId <= maxFieldId",
            ID,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"), fld("c", "long", 3, "col-3"))),
        // Field without an id
        rejected(
            "name mode rename and a field without an id",
            "is missing column id",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"), fld("c", "long", null, "col-c"))),
        rejected(
            "id mode rename and a field without an id",
            "is missing column id",
            ID,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"), fld("c", "long", null, "col-c"))),
        // Struct renamed, nested field replaced
        rejected(
            "struct rename with a nested id at or below maxColumnId",
            "fieldId <= maxFieldId",
            ID,
            true,
            cols(fld("s", struct(fld("x", "long", 2, "col-x")), 1, "col-s")),
            cols(fld("t", struct(fld("x2", "long", 3, "col-x2")), 1, "col-s"))),
        ok(
            "struct rename with a nested field replaced by a new id",
            ID,
            true,
            cols(fld("s", struct(fld("x", "long", 2, "col-x")), 1, "col-s")),
            cols(fld("t", struct(fld("x2", "long", 6, "col-x2")), 1, "col-s"))),
        // New physical name under the same id without a top-level rename
        rejected(
            "id mode keeps the id with a new physical name",
            "which is different from col-new",
            ID,
            false,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("a", "long", 1, "col-new"))),
        rejected(
            "array element field with a new physical name",
            "which is different from col-z",
            NAME,
            false,
            cols(fld("e", array(struct(fld("x", "long", 2, "col-x"))), 1, "col-e")),
            cols(fld("e", array(struct(fld("x", "long", 2, "col-z"))), 1, "col-e"))),
        rejected(
            "map value field with a new physical name",
            "which is different from col-z",
            NAME,
            false,
            cols(fld("m", map("string", struct(fld("x", "long", 2, "col-x"))), 1, "col-m")),
            cols(fld("m", map("string", struct(fld("x", "long", 2, "col-z"))), 1, "col-m"))),
        // No false positives next to repeated nested names
        ok(
            "name mode rename next to structs sharing a nested physical name",
            NAME,
            true,
            cols(
                fld("a", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s"),
                fld("t", struct(fld("x", "long", 5, "col-x")), 4, "col-t")),
            cols(
                fld("b", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s"),
                fld("t", struct(fld("x", "long", 5, "col-x")), 4, "col-t"))),
        ok(
            "id mode rename next to structs sharing a nested physical name",
            ID,
            true,
            cols(
                fld("a", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s"),
                fld("t", struct(fld("x", "long", 5, "col-x")), 4, "col-t")),
            cols(
                fld("b", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s"),
                fld("t", struct(fld("x", "long", 5, "col-x")), 4, "col-t"))),
        ok(
            "name mode rename next to the same name at another depth",
            NAME,
            true,
            cols(
                fld("x", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s")),
            cols(
                fld("y", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s"))),
        ok(
            "id mode rename next to the same name at another depth",
            ID,
            true,
            cols(
                fld("x", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s")),
            cols(
                fld("y", "long", 1, "col-1"),
                fld("s", struct(fld("x", "long", 3, "col-x")), 2, "col-s"))),
        // Clustering columns, stored as physical paths
        rejected(
            "rename while dropping a top-level clustering column",
            "Cannot drop clustering column c",
            with(NAME, "clusteringColumns", "[[\"col-c\"]]"),
            true,
            cols(fld("a", "long", 1, "col-1"), fld("c", "long", 2, "col-c")),
            cols(fld("b", "long", 1, "col-1"))),
        rejected(
            "rename while dropping a nested clustering column",
            "Cannot drop clustering column x",
            with(NAME, "clusteringColumns", "[[\"col-s\",\"col-x\"]]"),
            true,
            cols(
                fld("a", "long", 1, "col-1"),
                fld(
                    "s",
                    struct(fld("x", "long", 3, "col-x"), fld("y", "long", 4, "col-y")),
                    2,
                    "col-s")),
            cols(
                fld("b", "long", 1, "col-1"),
                fld("s", struct(fld("y", "long", 4, "col-y")), 2, "col-s"))),
        ok(
            "rename while dropping an unclustered field that shares a clustering physical name",
            CLUSTERED_BY_S_X,
            true,
            cols(
                fld("a", "long", 1, "col-1"),
                fld(
                    "s",
                    struct(fld("x", "long", 3, "col-x"), fld("y", "long", 6, "col-sy")),
                    2,
                    "col-s"),
                fld(
                    "t",
                    struct(fld("x", "long", 5, "col-x"), fld("y", "long", 7, "col-ty")),
                    4,
                    "col-t")),
            cols(
                fld("b", "long", 1, "col-1"),
                fld(
                    "s",
                    struct(fld("x", "long", 3, "col-x"), fld("y", "long", 6, "col-sy")),
                    2,
                    "col-s"),
                fld("t", struct(fld("y", "long", 7, "col-ty")), 4, "col-t"))),
        rejected(
            "rename while dropping the clustered field that shares a physical name with another",
            "Cannot drop clustering column x",
            CLUSTERED_BY_S_X,
            true,
            cols(
                fld("a", "long", 1, "col-1"),
                fld(
                    "s",
                    struct(fld("x", "long", 3, "col-x"), fld("y", "long", 6, "col-sy")),
                    2,
                    "col-s"),
                fld(
                    "t",
                    struct(fld("x", "long", 5, "col-x"), fld("y", "long", 7, "col-ty")),
                    4,
                    "col-t")),
            cols(
                fld("b", "long", 1, "col-1"),
                fld("s", struct(fld("y", "long", 6, "col-sy")), 2, "col-s"),
                fld(
                    "t",
                    struct(fld("x", "long", 5, "col-x"), fld("y", "long", 7, "col-ty")),
                    4,
                    "col-t"))),
        ok(
            "clustering path naming no field",
            with(NAME, "clusteringColumns", "[[\"col-1\",\"nope\"]]"),
            true,
            cols(fld("a", "long", 1, "col-1"), fld("c", "long", 2, "col-c")),
            cols(fld("b", "long", 1, "col-1"))),
        ok(
            "empty clustering path",
            with(NAME, "clusteringColumns", "[[]]"),
            true,
            cols(fld("a", "long", 1, "col-1"), fld("c", "long", 2, "col-c")),
            cols(fld("b", "long", 1, "col-1"))),
        ok(
            "clustering paths with null entries",
            with(NAME, "clusteringColumns", "[null,[null],[\"col-1\",null]]"),
            true,
            cols(fld("a", "long", 1, "col-1"), fld("c", "long", 2, "col-c")),
            cols(fld("b", "long", 1, "col-1"))),
        ok(
            "clustering property that is the json value null",
            with(NAME, "clusteringColumns", "null"),
            true,
            cols(fld("a", "long", 1, "col-1"), fld("c", "long", 2, "col-c")),
            cols(fld("b", "long", 1, "col-1"))),
        rejected(
            "clustering property that is not json",
            "clusteringColumns",
            with(NAME, "clusteringColumns", "not json"),
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"))),
        ok(
            "corrupt row-tracking high-water mark does not affect a rename",
            with(NAME, "delta.rowTracking.rowIdHighWaterMark", "not a number"),
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"))),
        // Other Kernel rejections
        rejected(
            "rename and a new NOT NULL field",
            "Cannot add non-nullable field",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"), fld("n", "long", false, 6, "col-6"))),
        rejected(
            "field moved to another struct",
            "Cannot move fields",
            NAME,
            true,
            cols(
                fld("s1", struct(fld("x", "long", 3, "col-x")), 1, "col-s1"),
                fld("s2", struct(fld("y", "long", 4, "col-y")), 2, "col-s2")),
            cols(
                fld("t1", struct(fld("z", "long", 6, "col-z")), 1, "col-s1"),
                fld(
                    "s2",
                    struct(fld("y", "long", 4, "col-y"), fld("x", "long", 3, "col-x")),
                    2,
                    "col-s2"))),
        rejected(
            "rename that rewrites delta.typeChanges",
            "Detected a modified type changes field",
            NAME,
            true,
            cols(typeChange(fld("a", "long", 1, "col-1"), "integer", "long")),
            cols(typeChange(fld("b", "long", 1, "col-1"), "short", "long"))),
        rejected(
            "duplicate id in the stored schema",
            "already exists",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1"), fld("c", "long", 1, "col-c")),
            cols(fld("b", "long", 1, "col-1"), fld("c", "long", 1, "col-c"))),
        // No rename and no id-preserving identity change: Kernel is skipped
        ok(
            "type change without a rename",
            NAME,
            false,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("a", "string", 1, "col-1"))),
        ok(
            "same name with a new id",
            ID,
            false,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("a", "long", 2, "col-1"))),
        // A malformed or out-of-range maxColumnId is a 400, not a 500
        rejected(
            "non-numeric maxColumnId",
            "abc",
            with(NAME, "delta.columnMapping.maxColumnId", "abc"),
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"))),
        rejected(
            "negative maxColumnId",
            "maxColumnId",
            with(NAME, "delta.columnMapping.maxColumnId", "-1"),
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"))),
        rejected(
            "maxColumnId above the int range",
            "maxColumnId",
            with(NAME, "delta.columnMapping.maxColumnId", "2147483648"),
            true,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"))),
        // The stored schema cannot be read
        rejected(
            "unreadable stored schema with a rename",
            "the table schema cannot be read",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1"), fld("v", "void", 2, "col-v")),
            cols(fld("b", "long", 1, "col-1"), fld("v", "void", 2, "col-v"))),
        ok(
            "unreadable stored schema without a rename",
            NAME,
            false,
            cols(fld("a", "long", 1, "col-1"), fld("v", "void", 2, "col-v")),
            cols(fld("a", "long", 1, "col-1"), fld("v", "void", 2, "col-v"))),
        // This server keeps no name-keyed column state, so Delta's answer stands.
        ok(
            "rename onto the name of a dropped column",
            NAME,
            true,
            cols(fld("a", "long", 1, "col-1"), fld("b", "long", 2, "col-2")),
            cols(fld("b", "long", 1, "col-1"))),
        // Property wiring: delta.enableTypeWidening and delta.enableIcebergWriterCompatV1
        ok(
            "rename with an allowed widening",
            with(NAME, "delta.enableTypeWidening", "true"),
            true,
            cols(fld("a", "integer", 1, "col-1")),
            cols(fld("b", "double", 1, "col-1"))),
        rejected(
            "widening Iceberg compat does not allow",
            "Cannot change the type of existing field",
            with(
                with(NAME, "delta.enableTypeWidening", "true"),
                "delta.enableIcebergWriterCompatV1",
                "true"),
            true,
            cols(fld("a", "integer", 1, "col-1")),
            cols(fld("b", "double", 1, "col-1"))),
        // A stored column whose type cannot be read has no identity; the gate skips it
        ok(
            "unreadable stored column is skipped by the identity gate",
            NAME,
            false,
            cols(fld("a", 12345, 1, "col-1")),
            cols(fld("b", "long", 1, "col-1"))),
        // A case-only rename runs Kernel and passes
        ok(
            "case-only rename",
            NAME,
            false,
            cols(fld("a", "long", 1, "col-1")),
            cols(fld("A", "long", 1, "col-1"))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  public void testValidate(Case c) throws Exception {
    List<ColumnInfo> current = columns(c.current());
    List<ColumnInfo> incoming = columns(c.incoming());
    Executable call =
        () ->
            DeltaSchemaEvolutionValidator.validate(
                current, incoming, c.properties(), c.renamesTopLevelColumn());
    if (c.rejection() == null) {
      assertDoesNotThrow(call);
      return;
    }
    BaseException e = assertThrows(BaseException.class, call);
    assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT);
    assertThat(e.getMessage()).contains(c.rejection());
  }
}
