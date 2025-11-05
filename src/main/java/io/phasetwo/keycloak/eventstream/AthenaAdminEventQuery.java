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
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AdminEventQuery;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.Datum;

@JBossLog
class AthenaAdminEventQuery extends AbstractAthenaQuery<AdminEvent> implements AdminEventQuery {

  private String realmId;
  private String authRealmId;
  private String authClientId;
  private String authUserId;
  private String authIpAddress;
  private final Set<OperationType> operationTypes = new LinkedHashSet<>();
  private final Set<ResourceType> resourceTypes = new LinkedHashSet<>();
  private String resourcePath;
  private Long fromTime;
  private Long toTime;
  private Integer firstResult;
  private Integer maxResults;
  private Order order = Order.DESC;

  AthenaAdminEventQuery(
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
  public AdminEventQuery realm(String realm) {
    this.realmId = realm;
    return this;
  }

  @Override
  public AdminEventQuery authRealm(String authRealm) {
    this.authRealmId = authRealm;
    return this;
  }

  @Override
  public AdminEventQuery authClient(String client) {
    this.authClientId = client;
    return this;
  }

  @Override
  public AdminEventQuery authUser(String user) {
    this.authUserId = user;
    return this;
  }

  @Override
  public AdminEventQuery authIpAddress(String ipAddress) {
    this.authIpAddress = ipAddress;
    return this;
  }

  @Override
  public AdminEventQuery operation(OperationType... operations) {
    if (operations != null) {
      operationTypes.addAll(Arrays.asList(operations));
    }
    return this;
  }

  @Override
  public AdminEventQuery resourceType(ResourceType... types) {
    if (types != null) {
      resourceTypes.addAll(Arrays.asList(types));
    }
    return this;
  }

  @Override
  public AdminEventQuery resourcePath(String path) {
    this.resourcePath = path;
    return this;
  }

  @Override
  public AdminEventQuery fromTime(Date time) {
    if (time != null) {
      this.fromTime = time.getTime();
    }
    return this;
  }

  @Override
  public AdminEventQuery fromTime(long time) {
    this.fromTime = time;
    return this;
  }

  @Override
  public AdminEventQuery toTime(Date time) {
    if (time != null) {
      this.toTime = time.getTime();
    }
    return this;
  }

  @Override
  public AdminEventQuery toTime(long time) {
    this.toTime = time;
    return this;
  }

  @Override
  public AdminEventQuery firstResult(int firstResult) {
    this.firstResult = firstResult >= 0 ? firstResult : null;
    return this;
  }

  @Override
  public AdminEventQuery maxResults(int maxResults) {
    this.maxResults = maxResults > 0 ? maxResults : null;
    return this;
  }

  @Override
  public AdminEventQuery orderByDescTime() {
    this.order = Order.DESC;
    return this;
  }

  @Override
  public AdminEventQuery orderByAscTime() {
    this.order = Order.ASC;
    return this;
  }

  @Override
  public Stream<AdminEvent> getResultStream() {
    String sql = buildSql();
    log.debugf("Executing Athena admin event query: %s", sql);
    return executeQuery(sql, this::mapRowToAdminEvent);
  }

  private String buildSql() {
    StringBuilder sql =
        new StringBuilder(
                "SELECT id, time, realmid, realmname, operationtype, resourcetype, resourcepath, representation, error, authrealmid, authrealmname, authclientid, authuserid, authipaddress, detailsjson FROM \"")
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

    if (realmId != null) {
      conditions.add("realmid = " + singleQuote(realmId));
    }

    if (authRealmId != null) {
      conditions.add("authrealmid = " + singleQuote(authRealmId));
    }

    if (authClientId != null) {
      conditions.add("authclientid = " + singleQuote(authClientId));
    }

    if (authUserId != null) {
      conditions.add("authuserid = " + singleQuote(authUserId));
    }

    if (authIpAddress != null) {
      conditions.add("authipaddress = " + singleQuote(authIpAddress));
    }

    if (!operationTypes.isEmpty()) {
      String opCsv =
          operationTypes.stream()
              .map(OperationType::name)
              .map(this::singleQuote)
              .collect(Collectors.joining(","));
      conditions.add("operationtype IN (" + opCsv + ")");
    }

    if (!resourceTypes.isEmpty()) {
      String resourceCsv =
          resourceTypes.stream()
              .map(ResourceType::name)
              .map(this::singleQuote)
              .collect(Collectors.joining(","));
      conditions.add("resourcetype IN (" + resourceCsv + ")");
    }

    if (resourcePath != null) {
      conditions.add("resourcepath = " + singleQuote(resourcePath));
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

  private AdminEvent mapRowToAdminEvent(List<Datum> data) {
    AdminEvent adminEvent = new AdminEvent();

    adminEvent.setId(stringValue(data, 0));

    String time = stringValue(data, 1);
    if (time != null) {
      try {
        adminEvent.setTime(Long.parseLong(time));
      } catch (NumberFormatException ex) {
        throw new IllegalStateException(
            "Invalid admin event time returned from Athena: " + time, ex);
      }
    }

    adminEvent.setRealmId(stringValue(data, 2));
    adminEvent.setRealmName(stringValue(data, 3));

    String op = stringValue(data, 4);
    if (op != null) {
      try {
        adminEvent.setOperationType(OperationType.valueOf(op));
      } catch (IllegalArgumentException ex) {
        throw new IllegalStateException("Unexpected operation type from Athena: " + op, ex);
      }
    }

    String resourceType = stringValue(data, 5);
    if (resourceType != null) {
      try {
        adminEvent.setResourceType(ResourceType.valueOf(resourceType));
      } catch (IllegalArgumentException ex) {
        // Not all resource types necessarily map to the enum; fall back to string value
        adminEvent.setResourceTypeAsString(resourceType);
      }
    }

    adminEvent.setResourcePath(stringValue(data, 6));
    adminEvent.setRepresentation(stringValue(data, 7));
    adminEvent.setError(stringValue(data, 8));

    String authRealm = stringValue(data, 9);
    String authRealmName = stringValue(data, 10);
    String authClient = stringValue(data, 11);
    String authUser = stringValue(data, 12);
    String authIp = stringValue(data, 13);
    if (authRealm != null
        || authRealmName != null
        || authClient != null
        || authUser != null
        || authIp != null) {
      AuthDetails authDetails = new AuthDetails();
      authDetails.setRealmId(authRealm);
      authDetails.setRealmName(authRealmName);
      authDetails.setClientId(authClient);
      authDetails.setUserId(authUser);
      authDetails.setIpAddress(authIp);
      adminEvent.setAuthDetails(authDetails);
    }

    Map<String, String> details = FlatEvents.deserializeDetails(stringValue(data, 14));
    if (details != null) {
      adminEvent.setDetails(details);
    }

    return adminEvent;
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
