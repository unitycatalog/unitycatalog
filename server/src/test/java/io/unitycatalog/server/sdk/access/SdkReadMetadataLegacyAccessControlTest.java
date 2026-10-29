package io.unitycatalog.server.sdk.access;

import io.unitycatalog.server.utils.ServerProperties.Property;

/** Metadata grants have the same limits while strict privilege checks are disabled. */
public class SdkReadMetadataLegacyAccessControlTest extends SdkReadMetadataAccessControlTest {

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.STRICT_PRIVILEGE_CHECKS_ENABLED.getKey(), "false");
  }
}
