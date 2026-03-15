package io.unitycatalog.spark.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

public class CredScopedFs extends DelegateToFileSystem {
  protected CredScopedFs(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(uri, new CredScopedFileSystem(), conf, uri.getScheme(), false);
  }
}
