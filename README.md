# flik-sql-demo


docker compose up --build -d
docker-compose up -d
docker compose run sql-client



# Table user_behavior
CREATE TABLE user_behavior_bounded (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    browser STRING,
    ts TIMESTAMP(3),
    proctime AS PROCTIME(),   -- generates processing-time attribute using computed column
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute
) WITH (
    'connector' = 'faker',
    'number-of-rows' = '500',
    'rows-per-second' = '100',
    'fields.user_id.expression' = '#{number.numberBetween ''10000'',''20000''}',
    'fields.item_id.expression' = '#{number.numberBetween ''1000'',''2000''}',
    'fields.category_id.expression' = '#{number.numberBetween ''1'',''11''}',
    'fields.behavior.expression' = '#{Options.option ''PV'',''FAV'',''CART'',''BUY'')}',
    'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
    'fields.ts.expression' = '#{date.past ''5'',''1'',''SECONDS''}'
);

SELECT * FROM user_behavior_bounded limit 10;

SELECT COUNT(*) FROM user_behavior_bounded;

SET 'execution.runtime-mode' = 'batch';

SELECT COUNT(*) FROM user_behavior_bounded;

set 'execution.runtime-mode' = 'streaming';

set 'sql-client.execution.result-mode' = 'changelog';

SELECT COUNT(*) FROM user_behavior_bounded;

set 'sql-client.execution.result-mode' = 'table';

# alter table user_behavior_bounded set ('number-of-rows' = '-1');

CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    browser STRING,
    ts TIMESTAMP(3),
    proctime AS PROCTIME(),   -- generates processing-time attribute using computed column
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- defines watermark on ts column, marks ts as event-time attribute
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '100',
    'fields.user_id.expression' = '#{number.numberBetween ''10000'',''20000''}',
    'fields.item_id.expression' = '#{number.numberBetween ''1000'',''2000''}',
    'fields.category_id.expression' = '#{number.numberBetween ''1'',''11''}',
    'fields.behavior.expression' = '#{Options.option ''PV'',''FAV'',''CART'',''BUY'')}',
    'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
    'fields.ts.expression' = '#{date.past ''5'',''1'',''SECONDS''}'
);

# Elastic search
<!-- CREATE TABLE buy_cnt_per_hour (
    hour_of_day BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = 'http://elasticsearch:9200',  -- elasticsearch address
    'index' = 'buy_cnt_per_hour'  -- elasticsearch index name, similar to database table name
);

INSERT INTO buy_cnt_per_hour
SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'BUY'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);

CREATE TABLE buy_cnt_per_minute (
    hour_of_day BIGINT,
    minute_of_hour BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = 'http://elasticsearch:9200',  -- elasticsearch address
    'index' = 'buy_cnt_per_minute'  -- elasticsearch index name, similar to database table name
);

INSERT INTO buy_cnt_per_minute
SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' MINUTE)), MINUTE(TUMBLE_START(ts, INTERVAL '1' MINUTE)), COUNT(*)
FROM user_behavior
WHERE behavior = 'BUY'
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE);
 -->
CREATE TABLE buy_cnt_per_5_secs (
    date_time TIMESTAMP,
    buy_cnt BIGINT,
    PRIMARY KEY (date_time) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7', -- using elasticsearch connector
    'hosts' = 'http://elasticsearch:9200',  -- elasticsearch address
    'index' = 'buy_cnt_per_5_secs'  -- elasticsearch index name, similar to database table name
);

INSERT INTO buy_cnt_per_5_secs
SELECT TUMBLE_START(ts, INTERVAL '5' SECOND), COUNT(*)
FROM user_behavior
WHERE behavior = 'BUY'
GROUP BY TUMBLE(ts, INTERVAL '5' SECOND);

<!-- CREATE TABLE cumulative_uv (
    date_str STRING,
    time_str STRING,
    uv BIGINT,
    PRIMARY KEY (date_str, time_str) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'cumulative_uv'
);

INSERT INTO cumulative_uv
SELECT date_str, MAX(time_str), COUNT(DISTINCT user_id) as uv
FROM (
  SELECT
    DATE_FORMAT(ts, 'yyyy-MM-dd') as date_str,
    SUBSTR(DATE_FORMAT(ts, 'HH:mm'),1,4) || '0' as time_str,
    user_id
  FROM user_behavior)
GROUP BY date_str, time_str; -->

CREATE TABLE cumulative_uv_10_secs (
    date_str STRING,
    time_str STRING,
    uv BIGINT,
    PRIMARY KEY (date_str, time_str) NOT ENFORCED
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'cumulative_uv_10_secs'
);

INSERT INTO cumulative_uv_10_secs
SELECT date_str, MAX(time_str), COUNT(DISTINCT user_id) as uv
FROM (
  SELECT
    DATE_FORMAT(ts, 'yyyy-MM-dd') as date_str,
    SUBSTR(DATE_FORMAT(ts, 'HH:mm:ss'),1,7) || '0' as time_str,
    user_id
  FROM user_behavior)
GROUP BY date_str, time_str;

# Mysql
CREATE TABLE category_dim (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/flink',
    'table-name' = 'category',
    'username' = 'root',
    'password' = '123456',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '10min'
);

# Elastic
CREATE TABLE top_category (
    category_name STRING PRIMARY KEY NOT ENFORCED,
    buy_cnt BIGINT
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'top_category'
);

CREATE VIEW rich_user_behavior AS
SELECT U.user_id, U.item_id, U.behavior, C.name as category_name
FROM user_behavior AS U LEFT JOIN category_dim FOR SYSTEM_TIME AS OF U.proctime AS C
ON U.category_id = C.id;

INSERT INTO top_category
SELECT category_name, COUNT(*) buy_cnt
FROM rich_user_behavior
WHERE behavior = 'BUY'
GROUP BY category_name;

# Confluent
confluent shell
confluent login
<!-- confluent environment create flink-sql-demo-env
confluent environment use <environment id>
confluent kafka cluster create flink-sql-demo-cluster --cloud aws --region eu-west-1 --type basic
confluent api-key create --resource <resource id>
+------------+------------------------------------------------------------------+
| API Key    | 74JJHGK2LMJ5ZZFG                                                 |
| API Secret | bMtgT++6k9Gs63FvfRHHp47lxKF1P87otmqq8qAA2AgAD1vOSYSHCUNnF9zo+tmi |
+------------+------------------------------------------------------------------+

confluent kafka topic create user_behavior --cluster <cluster id> -->
confluent kafka topic create user_behavior --cluster lkc-v110vn

<!-- CREATE TABLE user_behavior_kafka (
  user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    browser STRING,
    ts TIMESTAMP(3),
    proctime TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.group.id' = 'demoGroup',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = '<BOOTSTRAP_SERVER>',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="<API_KEY>" password="<API_SECRET>";',
  'value.format' = 'json',
  'sink.partitioner' = 'fixed'
); -->

CREATE TABLE user_behavior_kafka (
  user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    browser STRING,
    ts TIMESTAMP(3),
    proctime TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.group.id' = 'demoGroup',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'pkc-z9doz.eu-west-1.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="74JJHGK2LMJ5ZZFG" password="bMtgT++6k9Gs63FvfRHHp47lxKF1P87otmqq8qAA2AgAD1vOSYSHCUNnF9zo+tmi";',
  'value.format' = 'json',
  'sink.partitioner' = 'fixed'
);

INSERT INTO user_behavior_kafka SELECT * FROM user_behavior;

confluent kafka topic consume user_behavior --api-key 74JJHGK2LMJ5ZZFG

SELECT * FROM user_behavior_kafka;
SELECT browser, COUNT(user_id) FROM user_behavior_kafka GROUP BY browser;

CREATE TABLE user_behavior_kafka_latest (
  user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    browser STRING,
    ts TIMESTAMP(3),
    proctime TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.group.id' = 'demoGroup',
  'scan.startup.mode' = 'latest-offset',
  'properties.bootstrap.servers' = '<BOOTSTRAP_SERVER>',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="<API_KEY>" password="<API_SECRET>";',
  'value.format' = 'json',
  'sink.partitioner' = 'fixed'
); 

# Watermarks
-- Create source table
CREATE TABLE mobile_usage ( 
    activity STRING, 
    client_ip STRING,
    ingest_time AS PROCTIME(),
    log_time TIMESTAMP_LTZ(3), 
    WATERMARK FOR log_time AS log_time - INTERVAL '15' SECONDS
) WITH (
  'connector' = 'faker', 
  'rows-per-second' = '50',
  'fields.activity.expression' = '#{regexify ''(open_push_message|discard_push_message|open_app|display_overview|change_settings)''}',
  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
  'fields.log_time.expression' =  '#{date.past ''45'',''10'',''SECONDS''}'
);

-- Create sink table for rows that are non-late
CREATE TABLE unique_users_per_window ( 
    window_start TIMESTAMP(3), 
    window_end TIMESTAMP(3),
    ip_addresses BIGINT
) WITH (
  'connector' = 'blackhole'
);

-- Create sink table for rows that are late
CREATE TABLE late_usage_events ( 
    activity STRING, 
    client_ip STRING,
    ingest_time TIMESTAMP_LTZ(3),
    log_time TIMESTAMP_LTZ(3), 
    current_watermark TIMESTAMP_LTZ(3)    
) WITH (
  'connector' = 'blackhole'
);

-- Create a view with non-late data
CREATE TEMPORARY VIEW mobile_data AS
    SELECT * FROM mobile_usage
    WHERE CURRENT_WATERMARK(log_time) IS NULL
          OR log_time > CURRENT_WATERMARK(log_time);

-- Create a view with late data
CREATE TEMPORARY VIEW late_mobile_data AS 
    SELECT * FROM mobile_usage
        WHERE CURRENT_WATERMARK(log_time) IS NOT NULL
              AND log_time <= CURRENT_WATERMARK(log_time);

BEGIN STATEMENT SET;

-- Send all rows that are non-late to the sink for data that's on time
INSERT INTO unique_users_per_window
    SELECT window_start, window_end, COUNT(DISTINCT client_ip) AS ip_addresses
      FROM TABLE(
        TUMBLE(TABLE mobile_data, DESCRIPTOR(log_time), INTERVAL '10' SECOND))
      GROUP BY window_start, window_end;

-- Send all rows that are late to the sink for late data
INSERT INTO late_usage_events
    SELECT *, CURRENT_WATERMARK(log_time) as current_watermark from late_mobile_data;
      
END;

# postgres
CREATE TABLE accident_claims (
    claim_id INT,
    claim_total FLOAT,
    claim_total_receipt VARCHAR(50),
    claim_currency VARCHAR(3),
    member_id INT,
    accident_date VARCHAR(20),
    accident_type VARCHAR(20),
    accident_detail VARCHAR(20),
    claim_date VARCHAR(20),
    claim_status VARCHAR(10),
    ts_created VARCHAR(20),
    ts_updated VARCHAR(20)
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'postgres',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'postgres',
  'schema-name' = 'claims',
  'table-name' = 'accident_claims'
 );

SELECT * FROM accident_claims;

SELECT accident_detail,
       SUM(claim_total) AS agg_claim_costs
FROM accident_claims
WHERE claim_status <> 'DENIED'
GROUP BY accident_detail;

cat ./postgres_datagen.sql | docker exec -i flink-cdc-postgres psql -U postgres -d postgres

# Utils
SHOW JOBS;
STOP JOB '<job_id>';

### TODO
ADD TUMBLING WINDOW ON QUERY TO KAFKA TOPIC