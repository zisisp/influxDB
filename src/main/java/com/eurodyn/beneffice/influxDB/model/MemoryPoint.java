package com.eurodyn.beneffice.influxDB.model;

import java.time.Instant;
import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

@Measurement(name = "memory")
public class MemoryPoint {

  @Column(name = "time")
  private Instant time;

  @Column(name = "name")
  private String name;

  @Column(name = "free")
  private Long free;

  @Column(name = "used")
  private Long used;

  @Column(name = "buffer")
  private Long buffer;
}