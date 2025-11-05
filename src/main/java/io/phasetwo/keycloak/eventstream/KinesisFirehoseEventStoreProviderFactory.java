package io.phasetwo.keycloak.eventstream;

import com.google.auto.service.AutoService;
import lombok.extern.jbosslog.JBossLog;
import org.keycloak.Config;
import org.keycloak.events.EventStoreProvider;
import org.keycloak.events.EventStoreProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.provider.EnvironmentDependentProviderFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.AthenaClientBuilder;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.FirehoseClientBuilder;

@JBossLog
@AutoService(EventStoreProviderFactory.class)
public class KinesisFirehoseEventStoreProviderFactory
    implements EventStoreProviderFactory, EnvironmentDependentProviderFactory {

  public static final String PROVIDER_ID = "ext-event-aws-firehose-store";

  private FirehoseClient firehose;
  private AthenaClient athena;
  private String firehoseUserEventsStream;
  private String firehoseAdminEventsStream;
  private String athenaDatabase;
  private String athenaUserEventsTable;
  private String athenaAdminEventsTable;
  private String athenaWorkGroup;
  private String athenaOutputLocation;
  private long athenaQueryPollIntervalMillis;
  private int athenaQueryMaxAttempts;

  @Override
  public String getId() {
    return PROVIDER_ID;
  }

  @Override
  public EventStoreProvider create(KeycloakSession session) {
    return new KinesisFirehoseEventStoreProvider(
        session,
        firehose,
        firehoseUserEventsStream,
        firehoseAdminEventsStream,
        athena,
        athenaDatabase,
        athenaUserEventsTable,
        athenaAdminEventsTable,
        athenaWorkGroup,
        athenaOutputLocation,
        athenaQueryPollIntervalMillis,
        athenaQueryMaxAttempts);
  }

  @Override
  public void init(Config.Scope scope) {
    this.firehoseUserEventsStream =
        scope.get("firehoseUserEventsStream", "keycloak-events-user-events");
    this.firehoseAdminEventsStream =
        scope.get("firehoseAdminEventsStream", "keycloak-events-admin-events");
    this.athenaDatabase = scope.get("athenaDatabase", "keycloak-events");
    this.athenaUserEventsTable = scope.get("athenaUserEventsTable", "keycloak-events-user-events");
    this.athenaAdminEventsTable =
        scope.get("athenaAdminEventsTable", "keycloak-events-admin-events");
    this.athenaWorkGroup = scope.get("athenaWorkGroup");
    this.athenaOutputLocation = scope.get("athenaOutputLocation");
    this.athenaQueryPollIntervalMillis = resolveLong(scope, "athenaQueryPollIntervalMillis", 1000L);
    this.athenaQueryMaxAttempts = (int) resolveLong(scope, "athenaQueryMaxAttempts", 60L);

    String profile = scope.get("awsProfile");
    String region = scope.get("awsRegion");

    AwsCredentialsProvider credentialsProvider =
        profile != null
            ? ProfileCredentialsProvider.create(profile)
            : DefaultCredentialsProvider.create();

    Region awsRegion = region != null ? Region.of(region) : null;

    FirehoseClientBuilder firehoseBuilder =
        FirehoseClient.builder().credentialsProvider(credentialsProvider);
    AthenaClientBuilder athenaBuilder =
        AthenaClient.builder().credentialsProvider(credentialsProvider);
    if (awsRegion != null) {
      firehoseBuilder.region(awsRegion);
      athenaBuilder.region(awsRegion);
    }

    this.firehose = firehoseBuilder.build();
    this.athena = athenaBuilder.build();

    log.infof(
        "Configured AWS event store: profile=%s, region=%s, firehoseUserEventsStream=%s, firehoseAdminEventsStream=%s, athenaDatabase=%s, athenaUserTable=%s, athenaAdminTable=%s, athenaWorkGroup=%s, athenaOutputLocation=%s",
        profile,
        region,
        firehoseUserEventsStream,
        firehoseAdminEventsStream,
        athenaDatabase,
        athenaUserEventsTable,
        athenaAdminEventsTable,
        athenaWorkGroup,
        athenaOutputLocation);
  }

  @Override
  public void postInit(KeycloakSessionFactory factory) {}

  @Override
  public boolean isSupported(Config.Scope scope) {
    boolean enabled = scope.getBoolean("firehoseEnabled", false);
    log.debugf("firehose enabled %s %b", scope.get("firehoseEnabled"), enabled);
    return enabled;
  }

  @Override
  public void close() {
    if (firehose != null) {
      firehose.close();
    }
    if (athena != null) {
      athena.close();
    }
  }

  private long resolveLong(Config.Scope scope, String key, long defaultValue) {
    String value = scope.get(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ex) {
      log.warnf("Invalid numeric configuration for %s: %s", key, value);
      return defaultValue;
    }
  }
}
