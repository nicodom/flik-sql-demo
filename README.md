# flik-sql-demo


docker compose up --build -d
docker-compose up -d
docker compose run sql-client



# Table user_behavior
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
    'rows-per-second' = '10',
    'fields.user_id.expression' = '#{number.numberBetween ''10000'',''20000''}',
    'fields.item_id.expression' = '#{number.numberBetween ''1000'',''2000''}',
    'fields.category_id.expression' = '#{number.numberBetween ''100'',''200''}',
    'fields.behavior.expression' = '#{Options.option ''PV'',''FAV'',''CART'',''BUY'')}',
    'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
    'fields.ts.expression' = '#{date.past ''5'',''1'',''SECONDS''}'
);

# Elastic search
CREATE TABLE buy_cnt_per_hour (
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

CREATE TABLE cumulative_uv (
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
GROUP BY date_str;



# Mysql
CREATE TABLE category_dim (
    sub_category_id BIGINT,
    parent_category_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/flink',
    'table-name' = 'category',
    'username' = 'root',
    'password' = '123456',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '10min'
);


# Utils
SHOW JOBS;
STOP JOB '<job_id>';

### TODO
INCREASE DOCKER RESOURCES!!!