# Integration tests

## Prerequisites

- Run `build/sbt clean package publishLocal spark/publishLocal` to publish spark connector to local maven cache
- When testing prerelease spark or delta versions, those need to be built and pushed to local maven cache separately
- Set up a testing S3 bucket and IAM role
    - IAM Role needs trust policy so that the identity running the UC server can assume
    - IAM role needs policy to read/write/list from s3 bucket (or at least a prefix)
    - Copy testing data to your S3 bucket [test numbers table](../etc/data/external/unity/default/tables/numbers/)
- Update integration test [server.properties](./etc/conf/server.properties)
  - You must set _at least_ `s3.bucketPath.0`, `s3.region.0`, and `s3.awsRoleArn.0`.

## Running tests

First, set up the UC server to test against:
```shell
# run from the integration-tests dir to use the testing configurations
cd integration-tests
# The UC Server will need to be able to assume your AWS IAM Role, so you may need to specify the AWS user or profile: 
export AWS_PROFILE=your-root-profile
# --or for a user identity--
# export AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/etc
../bin/start-uc-server
```

Next, create a catalog for testing
```
bin/uc catalog create --name unity
```


Finally, run the tests
```shell
build/sbt integrationTests/test
```
 