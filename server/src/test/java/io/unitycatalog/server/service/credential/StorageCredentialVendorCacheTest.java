package io.unitycatalog.server.service.credential;

import static io.unitycatalog.server.service.credential.CredentialContext.READ_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import io.unitycatalog.server.service.credential.cache.StorageCredentialCache;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class StorageCredentialVendorCacheTest {

  private static final NormalizedURL LOC = NormalizedURL.from("s3://bucket/tableA");

  @Test
  void repeatedVendForSameBindingHitsCloudOnce() {
    CredentialDAO dao = mock(CredentialDAO.class);
    when(dao.getAwsIamRoleResponse()).thenReturn(new AwsIamRoleResponse().roleArn("arn:role/A"));

    ExternalLocationUtils elu = mock(ExternalLocationUtils.class);
    when(elu.getExternalLocationCredentialDaoForPath(any())).thenReturn(Optional.of(dao));

    CloudCredentialVendor cloud = mock(CloudCredentialVendor.class);
    when(cloud.vendCredential(any()))
        .thenReturn(
            new TemporaryCredentials()
                .awsTempCredentials(new AwsCredentials().accessKeyId("AK"))
                .expirationTime(System.currentTimeMillis() + 3_600_000L));

    StorageCredentialCache cache =
        new StorageCredentialCache(cloud, new ServerProperties(new Properties()));
    StorageCredentialVendor vendor = new StorageCredentialVendor(cache, elu);

    TemporaryCredentials first = vendor.vendCredential(LOC, READ_ONLY);
    TemporaryCredentials second = vendor.vendCredential(LOC, READ_ONLY);

    assertEquals(LOC.toString(), first.getUrl());
    assertEquals(LOC.toString(), second.getUrl());
    verify(cloud, times(1)).vendCredential(any()); // DB lookup twice, cloud vend once
    verify(elu, times(2)).getExternalLocationCredentialDaoForPath(any());
  }
}
