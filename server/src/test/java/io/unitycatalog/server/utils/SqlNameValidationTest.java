package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import org.junit.Assert;
import org.junit.Test;

public class SqlNameValidationTest {

  @Test
  public void testValidateSqlObjectName() {
    ValidationUtils.validateSqlObjectName("test");
    ValidationUtils.validateSqlObjectName("sample_volume_name");
    Assert.assertThrows(BaseException.class, () -> ValidationUtils.validateSqlObjectName("test."));
    Assert.assertThrows(BaseException.class, () -> ValidationUtils.validateSqlObjectName("test "));
    Assert.assertThrows(
        BaseException.class, () -> ValidationUtils.validateSqlObjectName("test/test"));
    char ctrlC = 3; // ASCII value for Control-C
    Assert.assertThrows(
        BaseException.class, () -> ValidationUtils.validateSqlObjectName("test" + ctrlC));
  }
}
