package io.unitycatalog.hadoop.internal.util;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import org.apache.hadoop.conf.Configuration;

public final class ClockUtil {
  private ClockUtil() {}

  public static Clock resolveClock(Configuration conf) {
    String clockName = conf.get(UCHadoopConfConstants.UC_TEST_CLOCK_NAME);
    return clockName != null ? Clock.getManualClock(clockName) : Clock.systemClock();
  }
}
