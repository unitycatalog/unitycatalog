# Releasing the Helm chart

Every Unity Catalog release X.Y.Z, patch releases included, publishes the chart with the same version. The chart runs
the server image `unitycatalog/unitycatalog:vX.Y.Z` and the UI image `unitycatalog/unitycatalog-ui:vX.Y.Z`, so both
images must be on Docker Hub before the chart is pushed.

Only the release managers, @tdas and @yili-db, can push to the `unitycatalog` Docker Hub organization.

## Publish the UI image

Publish the server image as for earlier releases. From the release tag, also publish the UI image with the same
version, even when `ui/` didn't change:

```sh
docker buildx build --platform linux/amd64,linux/arm64 -t unitycatalog/unitycatalog-ui:vX.Y.Z --push ui
```

If the build reports that multi-platform builds aren't supported, run `docker buildx create --use` first.

## Publish the chart

From the release tag, after the images are on Docker Hub:

```sh
helm/package-chart.sh X.Y.Z
helm registry login registry-1.docker.io
helm push unitycatalog-chart-X.Y.Z.tgz oci://registry-1.docker.io/unitycatalog
```

`package-chart.sh` sets the chart version to X.Y.Z and the app version to vX.Y.Z. Don't edit `Chart.yaml` for a
release.

## Verify

Log out first (`helm registry logout registry-1.docker.io`), so the check also confirms the chart is public. Then check
that every image the published chart runs exists, including the optional UI:

```sh
images=$(helm template uc oci://registry-1.docker.io/unitycatalog/unitycatalog-chart \
  --version X.Y.Z --set ui.enabled=true \
  | sed -n 's/^ *image: "\{0,1\}\([^"]*\)"\{0,1\}$/\1/p' | sort -u)
missing=0
while read -r image; do
  docker buildx imagetools inspect "$image" > /dev/null || { echo "Missing: $image"; missing=1; }
done <<< "$images"
[ -n "$images" ] && [ "$missing" = 0 ] && echo "All images exist"
```
