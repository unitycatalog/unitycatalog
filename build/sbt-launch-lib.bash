#!/usr/bin/env bash
#

# A library to simplify using the SBT launcher from other packages.
# Note: This should be used by tools like giter8/conscript etc.

# TODO - Should we merge the main SBT script with this library?

if test -z "$HOME"; then
  declare -r script_dir="$(dirname "$script_path")"
else
  declare -r script_dir="$HOME/.sbt"
fi

declare -a residual_args
declare -a java_args
declare -a scalac_args
declare -a sbt_commands
declare -a maven_profiles

if test -x "$JAVA_HOME/bin/java"; then
    echo -e "Using $JAVA_HOME as default JAVA_HOME."
    echo "Note, this will be overridden by -java-home if it is set."
    declare java_cmd="$JAVA_HOME/bin/java"
else
    declare java_cmd=java
fi

echoerr () {
  echo 1>&2 "$@"
}
vlog () {
  [[ $verbose || $debug ]] && echoerr "$@"
}
dlog () {
  [[ $debug ]] && echoerr "$@"
}

# The known-good SHA-256 of each sbt-launch jar this script is allowed to run, keyed by
# sbt.version. Recorded by downloading the jar from Maven Central and checking it against the
# .sha1 Maven Central publishes alongside it, then hashing the same (verified) bytes with SHA-256.
# An sbt upgrade (project/build.properties) must add a matching entry here in the same change, or
# every build/sbt invocation fails closed instead of running an unverified launcher.
sbt_launch_sha256 () {
  case "$1" in
    1.9.9) echo "99fd0509060b9655ece7c5a34ecd7ecb471986d4191922966100057f6b6f373c" ;;
    *) echo "" ;;
  esac
}

# Prints the SHA-256 of $1 as a lowercase hex string, using whichever of sha256sum (Linux) or
# shasum -a 256 (macOS) is on PATH.
sha256_of () {
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum > /dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echoerr "Neither sha256sum nor shasum is available to verify the sbt launcher checksum."
    exit 1
  fi
}

# Verifies $1 (a freshly downloaded "*.part" file) against the expected SHA-256 $3, and moves it
# to $2 only on a match. On a mismatch, deletes $1 and prints the expected and actual hashes so
# the caller can retry from another URL or fail.
verify_and_install_sbt_jar () {
  local jar_dl="$1"
  local jar_dest="$2"
  local expected_sha256="$3"
  local actual_sha256
  actual_sha256="$(sha256_of "$jar_dl")"
  if [[ "$actual_sha256" == "$expected_sha256" ]]; then
    mv "$jar_dl" "$jar_dest"
    return 0
  fi
  echoerr "sbt launcher checksum mismatch for $jar_dl"
  echoerr "  expected sha256: $expected_sha256"
  echoerr "  actual sha256:   $actual_sha256"
  rm -f "$jar_dl"
  return 1
}

acquire_sbt_jar () {
  SBT_VERSION=`awk -F "=" '/sbt\.version/ {print $2}' ./project/build.properties`

  SBT_LAUNCH_SHA256="$(sbt_launch_sha256 "${SBT_VERSION}")"
  if [[ -z "${SBT_LAUNCH_SHA256}" ]]; then
    echoerr "No known-good SHA-256 is recorded for sbt-launch ${SBT_VERSION} in sbt_launch_sha256()."
    echoerr "Add one in build/sbt-launch-lib.bash (verified against the .sha1 Maven Central publishes) when bumping sbt.version."
    exit 1
  fi

  # Download sbt from mirror URL if the environment variable is provided
  if [[ "${SBT_VERSION}" == "0.13.18" ]] && [[ -n "${SBT_MIRROR_JAR_URL}" ]]; then
    URL1="${SBT_MIRROR_JAR_URL}"
  elif [[ "${SBT_VERSION}" == "1.5.5" ]] && [[ -n "${SBT_1_5_5_MIRROR_JAR_URL}" ]]; then
    URL1="${SBT_1_5_5_MIRROR_JAR_URL}"
  else
    URL1=${DEFAULT_ARTIFACT_REPOSITORY:-${MAVEN_PROXY_URL:-https://maven-central.storage-download.googleapis.com/maven2}}/org/scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch-${SBT_VERSION}.jar
  fi
  BACKUP_URL="https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch-${SBT_VERSION}.jar"

  JAR=build/sbt-launch-${SBT_VERSION}.jar
  sbt_jar=$JAR

  if [[ ! -f "$sbt_jar" ]]; then
    # Download sbt launch jar if it hasn't been downloaded yet
    if [ ! -f "${JAR}" ]; then
    # Download
    printf 'Attempting to fetch sbt from %s\n' "${URL1}"
    JAR_DL="${JAR}.part"
    if [ $(command -v curl) ]; then
      curl --fail --location --silent ${URL1} > "${JAR_DL}" &&\
        verify_and_install_sbt_jar "${JAR_DL}" "${JAR}" "${SBT_LAUNCH_SHA256}"
    elif [ $(command -v wget) ]; then
      wget --quiet ${URL1} -O "${JAR_DL}" &&\
        verify_and_install_sbt_jar "${JAR_DL}" "${JAR}" "${SBT_LAUNCH_SHA256}"
    else
      printf "You do not have curl or wget installed, please install sbt manually from https://www.scala-sbt.org/\n"
      exit -1
    fi
    fi
    if [ ! -f "${JAR}" ]; then
    # Primary download failed (or failed checksum verification), retry from Maven Central directly
    printf 'Download from %s failed. Retrying from %s\n' "${URL1}" "${BACKUP_URL}"
    JAR_DL="${JAR}.part"
    if [ $(command -v curl) ]; then
      curl --fail --location --silent ${BACKUP_URL} > "${JAR_DL}" &&\
        verify_and_install_sbt_jar "${JAR_DL}" "${JAR}" "${SBT_LAUNCH_SHA256}"
    elif [ $(command -v wget) ]; then
      wget --quiet ${BACKUP_URL} -O "${JAR_DL}" &&\
        verify_and_install_sbt_jar "${JAR_DL}" "${JAR}" "${SBT_LAUNCH_SHA256}"
    fi
    fi
    if [ ! -f "${JAR}" ]; then
    # Both downloads failed, or neither one passed checksum verification
    printf "Our attempt to download sbt locally to ${JAR} failed. Please install sbt manually from https://www.scala-sbt.org/\n"
    exit -1
    fi
    printf "Launching sbt from ${JAR}\n"
  fi
}

execRunner () {
  # print the arguments one to a line, quoting any containing spaces
  [[ $verbose || $debug ]] && echo "# Executing command line:" && {
    for arg; do
      if printf "%s\n" "$arg" | grep -q ' '; then
        printf "\"%s\"\n" "$arg"
      else
        printf "%s\n" "$arg"
      fi
    done
    echo ""
  }

  "$@"
}

addJava () {
  dlog "[addJava] arg = '$1'"
  java_args=( "${java_args[@]}" "$1" )
}

enableProfile () {
  dlog "[enableProfile] arg = '$1'"
  maven_profiles=( "${maven_profiles[@]}" "$1" )
  export SBT_MAVEN_PROFILES="${maven_profiles[@]}"
}

addSbt () {
  dlog "[addSbt] arg = '$1'"
  sbt_commands=( "${sbt_commands[@]}" "$1" )
}
addResidual () {
  dlog "[residual] arg = '$1'"
  residual_args=( "${residual_args[@]}" "$1" )
}
addDebugger () {
  addJava "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$1"
}

# a ham-fisted attempt to move some memory settings in concert
# so they need not be dicked around with individually.
get_mem_opts () {
  local mem=${1:-1000}
  local perm=$(( $mem / 4 ))
  (( $perm > 256 )) || perm=256
  (( $perm < 4096 )) || perm=4096
  local codecache=$(( $perm / 2 ))

  echo "-Xms${mem}m -Xmx${mem}m -XX:ReservedCodeCacheSize=${codecache}m"
}

require_arg () {
  local type="$1"
  local opt="$2"
  local arg="$3"
  if [[ -z "$arg" ]] || [[ "${arg:0:1}" == "-" ]]; then
    echo "$opt requires <$type> argument" 1>&2
    exit 1
  fi
}

is_function_defined() {
  declare -f "$1" > /dev/null
}

process_args () {
  while [[ $# -gt 0 ]]; do
    case "$1" in
       -h|-help) usage; exit 1 ;;
    -v|-verbose) verbose=1 && shift ;;
      -d|-debug) debug=1 && shift ;;

           -ivy) require_arg path "$1" "$2" && addJava "-Dsbt.ivy.home=$2" && shift 2 ;;
           -mem) require_arg integer "$1" "$2" && sbt_mem="$2" && shift 2 ;;
     -jvm-debug) require_arg port "$1" "$2" && addDebugger $2 && shift 2 ;;
         -batch) exec </dev/null && shift ;;

       -sbt-jar) require_arg path "$1" "$2" && sbt_jar="$2" && shift 2 ;;
   -sbt-version) require_arg version "$1" "$2" && sbt_version="$2" && shift 2 ;;
     -java-home) require_arg path "$1" "$2" && java_cmd="$2/bin/java" && export JAVA_HOME=$2 && shift 2 ;;

            -D*) addJava "$1" && shift ;;
            -J*) addJava "${1:2}" && shift ;;
            -P*) enableProfile "$1" && shift ;;
              *) addResidual "$1" && shift ;;
    esac
  done

  is_function_defined process_my_args && {
    myargs=("${residual_args[@]}")
    residual_args=()
    process_my_args "${myargs[@]}"
  }
}

run() {
  # no jar? download it.
  [[ -f "$sbt_jar" ]] || acquire_sbt_jar "$sbt_version" || {
    # still no jar? uh-oh.
    echo "Download failed. Obtain the sbt-launch.jar manually and place it at $sbt_jar"
    exit 1
  }

  # process the combined args, then reset "$@" to the residuals
  process_args "$@"
  set -- "${residual_args[@]}"
  argumentCount=$#

  # run sbt
  execRunner "$java_cmd" \
    ${SBT_OPTS:-$default_sbt_opts} \
    $(get_mem_opts $sbt_mem) \
    ${java_opts} \
    ${java_args[@]} \
    -jar "$sbt_jar" \
    "${sbt_commands[@]}" \
    "${residual_args[@]}"
}
