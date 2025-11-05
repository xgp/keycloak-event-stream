# keycloak-event-stream

Keycloak `EventStoreProvider` implemented using AWS Firehose, S3 and Athena.

## Configuration

Enable the provider by setting the Keycloak option `--spi-events-store-provider=ext-event-aws-firehose-store` and toggling it on with `--spi-events-store-ext-event-aws-firehose-store-firehose-enabled=true`. The factory recognises the following configuration keys (provided via the Keycloak SPI naming convention `--spi-events-store-ext-event-aws-firehose-store-<key>=<value>`):

| Key | Default | Description |
| --- | --- | --- |
| `firehoseUserEventsStream` | `keycloak-events-user-events` | Name of the Kinesis Firehose stream used for user events. |
| `firehoseAdminEventsStream` | `keycloak-events-admin-events` | Firehose stream that receives admin events. |
| `athenaDatabase` | `keycloak-events` | Athena database containing the Glue tables defined in Terraform. |
| `athenaUserEventsTable` | `keycloak-events-user-events` | Athena table queried for user events. |
| `athenaAdminEventsTable` | `keycloak-events-admin-events` | Athena table queried for admin events. |
| `athenaWorkGroup` | — | Athena workgroup to run queries under. |
| `athenaOutputLocation` | — | S3 URI for query results when the chosen workgroup requires it. |
| `athenaQueryPollIntervalMillis` | `1000` | Poll interval, in milliseconds, while waiting for Athena query completion. |
| `athenaQueryMaxAttempts` | `60` | Maximum polling attempts before timing out Athena queries. |
| `awsProfile` | — | Named AWS credentials profile for both Firehose and Athena clients. |
| `awsRegion` | SDK default | AWS region for Firehose and Athena clients. |
| `firehoseEnabled` | `false` | Feature flag read by `isSupported` to decide whether the provider should load. |
