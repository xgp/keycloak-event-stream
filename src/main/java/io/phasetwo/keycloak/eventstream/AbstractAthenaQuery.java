package io.phasetwo.keycloak.eventstream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

abstract class AbstractAthenaQuery<T> {

  private final AthenaClient athenaClient;
  private final String database;
  private final String table;
  private final String workGroup;
  private final String outputLocation;
  private final long pollIntervalMillis;
  private final int maxAttempts;

  AbstractAthenaQuery(
      AthenaClient athenaClient,
      String database,
      String table,
      String workGroup,
      String outputLocation,
      long pollIntervalMillis,
      int maxAttempts) {
    this.athenaClient = Objects.requireNonNull(athenaClient, "athenaClient");
    this.database = Objects.requireNonNull(database, "database");
    this.table = Objects.requireNonNull(table, "table");
    this.workGroup = workGroup;
    this.outputLocation = outputLocation;
    this.pollIntervalMillis =
        pollIntervalMillis > 0 ? pollIntervalMillis : Duration.ofSeconds(1).toMillis();
    this.maxAttempts = maxAttempts > 0 ? maxAttempts : 60;
  }

  protected String table() {
    return table;
  }

  protected Stream<T> executeQuery(String sql, Function<List<Datum>, T> rowMapper) {
    StartQueryExecutionResponse startResponse =
        athenaClient.startQueryExecution(buildStartQueryRequest(sql));
    String queryExecutionId = startResponse.queryExecutionId();
    waitForCompletion(queryExecutionId);

    List<T> results = new ArrayList<>();
    String nextToken = null;
    boolean skipHeader = true;
    do {
      GetQueryResultsResponse response =
          athenaClient.getQueryResults(
              GetQueryResultsRequest.builder()
                  .queryExecutionId(queryExecutionId)
                  .nextToken(nextToken)
                  .build());

      List<Row> rows = response.resultSet().rows();
      if (rows != null) {
        for (int i = skipHeader ? 1 : 0; i < rows.size(); i++) {
          Row row = rows.get(i);
          if (row.data() == null || row.data().isEmpty()) {
            continue;
          }
          results.add(rowMapper.apply(row.data()));
        }
      }

      nextToken = response.nextToken();
      skipHeader = false;
    } while (nextToken != null);

    return results.stream();
  }

  private StartQueryExecutionRequest buildStartQueryRequest(String sql) {
    StartQueryExecutionRequest.Builder builder =
        StartQueryExecutionRequest.builder().queryString(sql);

    builder.queryExecutionContext(QueryExecutionContext.builder().database(database).build());

    ResultConfiguration.Builder resultConfigBuilder = ResultConfiguration.builder();
    if (outputLocation != null && !outputLocation.isBlank()) {
      resultConfigBuilder.outputLocation(outputLocation);
    }
    builder.resultConfiguration(resultConfigBuilder.build());

    if (workGroup != null && !workGroup.isBlank()) {
      builder.workGroup(workGroup);
    }

    return builder.build();
  }

  private void waitForCompletion(String queryExecutionId) {
    int attempts = 0;
    while (attempts < maxAttempts) {
      GetQueryExecutionResponse response =
          athenaClient.getQueryExecution(
              GetQueryExecutionRequest.builder().queryExecutionId(queryExecutionId).build());

      QueryExecutionState state = response.queryExecution().status().state();
      switch (state) {
        case SUCCEEDED:
          return;
        case FAILED:
        case CANCELLED:
          throw new IllegalStateException(
              "Athena query "
                  + queryExecutionId
                  + " finished with state "
                  + state
                  + ": "
                  + response.queryExecution().status().stateChangeReason());
        default:
          try {
            Thread.sleep(pollIntervalMillis);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "Interrupted while waiting for Athena query to finish", e);
          }
          attempts++;
      }
    }

    throw new IllegalStateException(
        "Timed out after " + maxAttempts + " attempts while waiting for Athena query to finish");
  }
}
