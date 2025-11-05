package io.phasetwo.keycloak.eventstream;

import java.util.Objects;
import lombok.extern.jbosslog.JBossLog;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerTransaction;
import org.keycloak.events.EventQuery;
import org.keycloak.events.EventStoreProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AdminEventQuery;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.util.JsonSerialization;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordRequest;
import software.amazon.awssdk.services.firehose.model.Record;

@JBossLog
public class KinesisFirehoseEventStoreProvider implements EventStoreProvider {

  private final EventListenerTransaction tx;
  private final FirehoseClient firehose;
  private final String firehoseUserEventsStream;
  private final String firehoseAdminEventsStream;
  private final AthenaClient athena;
  private final String athenaDatabase;
  private final String athenaUserEventsTable;
  private final String athenaAdminEventsTable;
  private final String athenaWorkGroup;
  private final String athenaOutputLocation;
  private final long queryPollIntervalMillis;
  private final int queryMaxAttempts;

  public KinesisFirehoseEventStoreProvider(
      KeycloakSession session,
      FirehoseClient firehose,
      String firehoseUserEventsStream,
      String firehoseAdminEventsStream,
      AthenaClient athena,
      String athenaDatabase,
      String athenaUserEventsTable,
      String athenaAdminEventsTable,
      String athenaWorkGroup,
      String athenaOutputLocation,
      long queryPollIntervalMillis,
      int queryMaxAttempts) {
    this.tx = new EventListenerTransaction(this::logAdminEvent, this::logEvent);
    this.firehose = Objects.requireNonNull(firehose, "firehose client is required");
    this.firehoseUserEventsStream = firehoseUserEventsStream;
    this.firehoseAdminEventsStream = firehoseAdminEventsStream;
    this.athena = Objects.requireNonNull(athena, "athena client is required");
    this.athenaDatabase = Objects.requireNonNull(athenaDatabase, "athena database is required");
    this.athenaUserEventsTable =
        Objects.requireNonNull(athenaUserEventsTable, "athena user events table is required");
    this.athenaAdminEventsTable =
        Objects.requireNonNull(athenaAdminEventsTable, "athena admin events table is required");
    this.athenaWorkGroup = athenaWorkGroup;
    this.athenaOutputLocation = athenaOutputLocation;
    this.queryPollIntervalMillis = queryPollIntervalMillis;
    this.queryMaxAttempts = queryMaxAttempts;
    session.getTransactionManager().enlistAfterCompletion(tx);
  }

  @Override
  public void onEvent(Event event) {
    log.debugf("Queueing user event %s", event.getId());
    tx.addEvent(event);
  }

  @Override
  public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
    log.debugf("Queueing admin event %s", adminEvent.getId());
    tx.addAdminEvent(adminEvent, includeRepresentation);
  }

  @Override
  public EventQuery createQuery() {
    return new AthenaEventQuery(
        athena,
        athenaDatabase,
        athenaUserEventsTable,
        athenaWorkGroup,
        athenaOutputLocation,
        queryPollIntervalMillis,
        queryMaxAttempts);
  }

  @Override
  public AdminEventQuery createAdminQuery() {
    return new AthenaAdminEventQuery(
        athena,
        athenaDatabase,
        athenaAdminEventsTable,
        athenaWorkGroup,
        athenaOutputLocation,
        queryPollIntervalMillis,
        queryMaxAttempts);
  }

  @Override
  public void clear() {}

  @Override
  public void clear(RealmModel realm) {}

  @Override
  public void clear(RealmModel realm, long olderThan) {}

  @Override
  public void clearExpiredEvents() {}

  @Override
  public void clearAdmin() {}

  @Override
  public void clearAdmin(RealmModel realm) {}

  @Override
  public void clearAdmin(RealmModel realm, long olderThan) {}

  protected void logEvent(Event event) {
    try {
      send(JsonSerialization.writeValueAsString(new FlatEvent(event)), firehoseUserEventsStream);
    } catch (Exception e) {
      log.warn("Error serializing user event", e);
    }
  }

  protected void logAdminEvent(AdminEvent adminEvent, boolean realmIncludeRepresentation) {
    try {
      send(
          JsonSerialization.writeValueAsString(new FlatAdminEvent(adminEvent)),
          firehoseAdminEventsStream);
    } catch (Exception e) {
      log.warn("Error serializing admin event", e);
    }
  }

  protected void send(String json, String stream) {
    try {
      log.debugf("Delivering record to %s", stream);
      Record record = Record.builder().data(SdkBytes.fromUtf8String(json + "\n")).build();
      firehose.putRecord(
          PutRecordRequest.builder().deliveryStreamName(stream).record(record).build());
    } catch (Exception e) {
      log.warn("Error sending to firehose", e);
    }
  }

  @Override
  public void close() {}
}
