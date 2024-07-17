package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SqlNameValidationTest {


    @Test
    public void testValidateSqlObjectName() {
        ValidationUtils.validateSqlObjectName("test");
        ValidationUtils.validateSqlObjectName("sample_volume_name");
        assertThrows(BaseException.class, () -> ValidationUtils.validateSqlObjectName("test."));
        assertThrows(BaseException.class, () -> ValidationUtils.validateSqlObjectName("test "));
        assertThrows(BaseException.class, () -> ValidationUtils.validateSqlObjectName("test/test"));
        char ctrlC = 3; // ASCII value for Control-C
        assertThrows(BaseException.class, () -> ValidationUtils.validateSqlObjectName("test" + ctrlC));
    }

}
