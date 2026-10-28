# Releasing the Helm chart

Every Unity Catalog release X.Y.Z, patch releases included, publishes the chart with the same version. The chart runs
the server image `unitycatalog/unitycatalog:vX.Y.Z` and the UI image `unitycatalog/unitycatalog-ui:vX.Y.Z`, so both
images must be on Docker Hub before the chart is pushed.

Only the release managers push to the `unitycatalog` Docker Hub organization, and only they should have write access to
its repositories. The release managers are among the maintainers listed in [`.github/CODEOWNERS`](../.github/CODEOWNERS).

## Bump the pinned helper images

The chart's helper images are pinned by digest in `helm/values.yaml`, so a release always runs the same images: Alpine
for the config init container and the create-users job, and busybox for the UI init container and the `helm test` pod.
Before each release, update both to their latest stable release in a pull request, and merge it before you create the
release tag so the tagged chart carries the new digests. Inspect the exact tags you pin, for example
`docker buildx imagetools inspect alpine:3.24.2` and `docker buildx imagetools inspect busybox:1.37.0`, not floating
tags such as `alpine:3`, which give a different digest.

## Publish the images

The chart needs the server image `unitycatalog/unitycatalog:vX.Y.Z` for both `linux/amd64` and `linux/arm64`, pushed
before the chart. From the release tag, also publish the UI image with the same version, even when `ui/` didn't change:

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

Check that every image the published chart runs exists, including the UI. The check, written for bash or zsh, runs
with empty credential files, so it fetches the chart and inspects the images logged out and also confirms that they
are public. Run it in its own shell session, because it sets environment variables. Helm also uses the credentials from `docker login`, so `helm registry logout` alone isn't enough.

```bash
export HELM_REGISTRY_CONFIG="$(mktemp -d)/config.json" DOCKER_CONFIG="$(mktemp -d)"
images=$(helm template uc oci://registry-1.docker.io/unitycatalog/unitycatalog-chart --version X.Y.Z \
  | sed -n 's/^ *image: "\{0,1\}\([^"]*\)"\{0,1\}$/\1/p' | sort -u)
missing=0
while read -r image; do
  [ -n "$image" ] || continue
  docker buildx imagetools inspect "$image" > /dev/null || { echo "Missing: $image"; missing=1; }
done <<< "$images"
[ -n "$images" ] && [ "$missing" = 0 ] && echo "All images exist"
```
