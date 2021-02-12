package com.eurodyn.beneffice.influxDB.service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InfluxDBService {

  private InfluxDB influxDB;

  @Value("${spring.influxdb.url}")
  private String databaseURL;

  @Value("${spring.influxdb.username}")
  private String userName;

  @Value("${spring.influxdb.password}")
  private String password;


  public void wireValues(String database, String changeFrom, String changeTo,
      String nameOfValue) {
    influxDB.setDatabase(database);
    QueryResult measurements = influxDB.query(new Query("Show measurements"));

    List<List<Object>> values = getValues(measurements);
    if (values != null) {
      values.forEach(
          value -> moveValue((String) value.get(0), database, changeFrom, changeTo, nameOfValue));
    }
  }

  private void moveValue(String measurement, String database, String changeFrom, String changeTo,
      String nameOfValue) {
    QueryResult query = influxDB.query(
        new Query(
            "Select * from " + measurement + " where " + nameOfValue + "='" + changeFrom + "'"));

    List<String> columns = getColumns(query);
    List<List<Object>> values = getValues(query);

    if (values == null || columns == null) {
      return;
    }

    Map<Integer, String> columnsMap = new HashMap<>();

    for (int i = 0; i < columns.size(); i++) {
      columnsMap.put(i, columns.get(i));
    }
    List<Point> points = new ArrayList<>();

    values.forEach(x -> points.add(createPoint(x, columnsMap, measurement, changeTo, nameOfValue)));

    BatchPoints batchPoints = BatchPoints
        .database(database)
        .build();

    for (int i = 0; i < points.size(); i++) {
      batchPoints.point(points.get(i));
      if (i % 10000 == 0 || i == points.size() - 1) {
        log.info("Writing batch points:" + batchPoints.getPoints().size());
        influxDB.write(batchPoints);
        //reset batch point
        batchPoints = BatchPoints.database(database)
            .build();
      }
    }
  }

  private List<List<Object>> getValues(QueryResult query) {
    List<Result> results = query.getResults();
    if (!results.isEmpty()) {
      Result result = results.get(0);
      if (result != null) {
        List<Series> series = result.getSeries();
        if (series != null && series.get(0) != null) {
          return series.get(0).getValues();
        }
      }
    }
    return null;
  }

  private List<String> getColumns(QueryResult query) {
    List<Result> results = query.getResults();
    Result result = results.get(0);
    if (query.getResults() != null && query.getResults().get(0) != null && result != null && result
        .getSeries() != null && result.getSeries().get(0) != null) {
      return result.getSeries().get(0).getColumns();
    } else {
      return null;
    }
  }

  private Point createPoint(List<Object> value, Map<Integer, String> columnsMap,
      String measurement, String to, String hardwareId) {
    Builder point = Point.measurement(measurement);
    List<String> weather_lastMeasurement = List
        .of("Weather_LastMeasurement", "Weather_CurrentCloudiness", "Weather_CurrentHumidity",
            "Weather_StationId");
    for (int i = 0; i < value.size(); i++) {
      if (columnsMap.get(i).equals("time")) {
        String time = value.get(i).toString();
        Instant instant = ZonedDateTime
            .parse(time, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("Europe/Berlin")))
            .toInstant();
        point.time(instant.toEpochMilli(), TimeUnit.MILLISECONDS);
      } else {
        if (columnsMap.get(i).contains(hardwareId)) {
          //use the new hardwareId
          point.addField(hardwareId, to);
        } else if (columnsMap.get(i).contains("value") && value.get(i) instanceof Double) {
          //use the new hardwareId
          if (weather_lastMeasurement.contains(measurement)) {
            point.addField(columnsMap.get(i), ((Double) value.get(i)).intValue());
          } else {
            point.addField(columnsMap.get(i), ((Double) value.get(i)).floatValue());
          }
        } else if (value.get(i) instanceof String) {
          point.addField(columnsMap.get(i), (String) value.get(i));
        } else if (value.get(i) instanceof Double) {
          point.addField(columnsMap.get(i), ((Double) value.get(i)).intValue());
        } else if (value.get(i) instanceof Long) {
          point.addField(columnsMap.get(i), (Long) value.get(i));
        } else if (value.get(i) instanceof Integer) {
          point.addField(columnsMap.get(i), (Integer) value.get(i));
        } else if (value.get(i) instanceof Float) {
          point.addField(columnsMap.get(i), (Float) value.get(i));
        }

      }
    }
    return point.build();
  }

  private void createDatabase(String databaseName) {
    OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder()
        .connectTimeout(2, TimeUnit.MINUTES)
        .readTimeout(3, TimeUnit.MINUTES)
        .writeTimeout(3, TimeUnit.MINUTES);
    influxDB = InfluxDBFactory.connect(databaseURL, userName, password, okHttpClientBuilder);
    Pong response = influxDB.ping();
    if (response.getVersion().equalsIgnoreCase("unknown")) {
      log.error("Error pinging server.");
      return;
    } else {
      log.info("influxdb connected:" + response.getVersion());
    }
//    influxDB.query(new Query(String.format("Create database %s", databaseName)));
//    influxDB.createRetentionPolicy(
//        "defaultPolicy", databaseName, "30d", 1, true);
//    influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
  }

  public void run(ApplicationArguments args) {
    log.info("Running application test");
//    influxDBTemplate.createDatabase();
    String database = args.getSourceArgs()[0];
    String from = args.getSourceArgs()[1];
    String to = args.getSourceArgs()[2];
    String nameOfValue = args.getSourceArgs()[3];
    createDatabase(database);
    wireValues(database, from, to,
        nameOfValue);
//    System.exit(0);
  }

  private void dropDatabase(String dbName) {
    QueryResult query = influxDB.query(new Query("Drop database " + dbName));

  }

  private void testRead() {

  }

  @PreDestroy
  public void closeConnection() {
    influxDB.close();
  }
}