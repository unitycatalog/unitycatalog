package io.unitycatalog.spark.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/**
 * An {@link org.apache.hadoop.fs.AbstractFileSystem} adapter for {@link CredScopedFileSystem}.
 *
 * <p>Hadoop's {@code fs.AbstractFileSystem.<scheme>.impl} config key requires an {@link
 * org.apache.hadoop.fs.AbstractFileSystem} subclass, not a {@link org.apache.hadoop.fs.FileSystem}.
 * This class bridges the gap by extending {@link DelegateToFileSystem} and delegating all
 * operations to a fresh {@link CredScopedFileSystem}, which in turn uses its static
 * credential-scoped cache to find or create the real delegate.
 *
 * <p>Registered via {@code fs.AbstractFileSystem.<scheme>.impl} in {@link
 * io.unitycatalog.spark.auth.CredPropsUtil} when {@code credScopedFs.enabled} is {@code true}.
 */
public class CredScopedFs extends DelegateToFileSystem {
  protected CredScopedFs(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(uri, new CredScopedFileSystem(), conf, uri.getScheme(), false);
  }
}
