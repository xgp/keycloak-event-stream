package io.phasetwo.keycloak.eventstream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.jbosslog.JBossLog;
import org.keycloak.events.Event;
import org.keycloak.events.EventQuery;
import org.keycloak.events.EventType;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.Datum;

@JBossLog
class AthenaEventQuery extends AbstractAthenaQuery<Event> implements EventQuery {

  private final Set<EventType> eventTypes = new LinkedHashSet<>();
  private String realmId;
  private String clientId;
  private String userId;
  private String ipAddress;
  private Long fromTime;
  private Long toTime;
  private Integer firstResult;
  private Integer maxResults;
  private Order order = Order.DESC;

  AthenaEventQuery(
      AthenaClient athenaClient,
      String database,
      String table,
      String workGroup,
      String outputLocation,
      long pollIntervalMillis,
      int maxAttempts) {
    super(
        athenaClient, database, table, workGroup, outputLocation, pollIntervalMillis, maxAttempts);
  }

  @Override
  public EventQuery type(EventType... types) {
    if (types != null) {
      eventTypes.addAll(Arrays.asList(types));
    }
    return this;
  }

  @Override
  public EventQuery realm(String realmId) {
    this.realmId = realmId;
    return this;
  }

  @Override
  public EventQuery client(String clientId) {
    this.clientId = clientId;
    return this;
  }

  @Override
  public EventQuery user(String userId) {
    this.userId = userId;
    return this;
  }

  @Override
  public EventQuery fromDate(Date fromDate) {
    if (fromDate != null) {
      this.fromTime = fromDate.getTime();
    }
    return this;
  }

  @Override
  public EventQuery fromDate(long fromDate) {
    this.fromTime = fromDate;
    return this;
  }

  @Override
  public EventQuery toDate(Date toDate) {
    if (toDate != null) {
      this.toTime = toDate.getTime();
    }
    return this;
  }

  @Override
  public EventQuery toDate(long toDate) {
    this.toTime = toDate;
    return this;
  }

  @Override
  public EventQuery ipAddress(String ipAddress) {
    this.ipAddress = ipAddress;
    return this;
  }

  @Override
  public EventQuery firstResult(int firstResult) {
    this.firstResult = firstResult >= 0 ? firstResult : null;
    return this;
  }

  @Override
  public EventQuery maxResults(int maxResults) {
    this.maxResults = maxResults > 0 ? maxResults : null;
    return this;
  }

  @Override
  public EventQuery orderByDescTime() {
    this.order = Order.DESC;
    return this;
  }

  @Override
  public EventQuery orderByAscTime() {
    this.order = Order.ASC;
    return this;
  }

  @Override
  public Stream<Event> getResultStream() {
    String sql = buildSql();
    log.debugf("Executing Athena event query: %s", sql);
    return executeQuery(sql, this::mapRowToEvent);
  }

  private String buildSql() {
    StringBuilder sql =
        new StringBuilder(
                "SELECT id, eventtype, realmid, realmname, clientid, userid, sessionid, ipaddress, error, time, detailsjson FROM \"")
            .append(table())
            .append("\"");

    List<String> conditions = buildConditions();
    if (!conditions.isEmpty()) {
      sql.append(" WHERE ").append(String.join(" AND ", conditions));
    }

    sql.append(" ORDER BY time ").append(order == Order.ASC ? "ASC" : "DESC");

    if (maxResults != null) {
      sql.append(" LIMIT ").append(maxResults);
    }

    if (firstResult != null && firstResult > 0) {
      sql.append(" OFFSET ").append(firstResult);
    }

    return sql.toString();
  }

  private List<String> buildConditions() {
    List<String> conditions = new ArrayList<>();

    if (!eventTypes.isEmpty()) {
      String typesCsv =
          eventTypes.stream()
              .map(EventType::name)
              .map(this::singleQuote)
              .collect(Collectors.joining(","));
      conditions.add("eventtype IN (" + typesCsv + ")");
    }

    if (realmId != null) {
      conditions.add("realmid = " + singleQuote(realmId));
    }

    if (clientId != null) {
      conditions.add("clientid = " + singleQuote(clientId));
    }

    if (userId != null) {
      conditions.add("userid = " + singleQuote(userId));
    }

    if (ipAddress != null) {
      conditions.add("ipaddress = " + singleQuote(ipAddress));
    }

    if (fromTime != null) {
      conditions.add("time >= " + fromTime);
    }

    if (toTime != null) {
      conditions.add("time <= " + toTime);
    }

    return conditions;
  }

  private String singleQuote(String value) {
    if (value == null) {
      return "NULL";
    }
    return "'" + value.replace("'", "''") + "'";
  }

  private Event mapRowToEvent(List<Datum> data) {
    Event event = new Event();

    event.setId(stringValue(data, 0));

    String eventType = stringValue(data, 1);
    if (eventType != null) {
      try {
        event.setType(EventType.valueOf(eventType));
      } catch (IllegalArgumentException ex) {
        throw new IllegalStateException("Unexpected event type from Athena: " + eventType, ex);
      }
    }

    event.setRealmId(stringValue(data, 2));
    event.setRealmName(stringValue(data, 3));
    event.setClientId(stringValue(data, 4));
    event.setUserId(stringValue(data, 5));
    event.setSessionId(stringValue(data, 6));
    event.setIpAddress(stringValue(data, 7));
    event.setError(stringValue(data, 8));

    String time = stringValue(data, 9);
    if (time != null) {
      try {
        event.setTime(Long.parseLong(time));
      } catch (NumberFormatException ex) {
        throw new IllegalStateException("Invalid event time returned from Athena: " + time, ex);
      }
    }

    Map<String, String> details = FlatEvents.deserializeDetails(stringValue(data, 10));
    if (details != null) {
      event.setDetails(details);
    }

    return event;
  }

  private String stringValue(List<Datum> data, int index) {
    if (index >= data.size()) {
      return null;
    }
    Datum datum = data.get(index);
    if (datum == null || datum.varCharValue() == null || datum.varCharValue().isBlank()) {
      return null;
    }
    return datum.varCharValue();
  }

  private enum Order {
    ASC,
    DESC
  }
}
