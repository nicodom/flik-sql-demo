# flik-sql-demo

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
    'rows-per-second' = '1',
    'fields.user_id.expression' = '#{number.numberBetween ''10000'',''20000''}',
    'fields.item_id.expression' = '#{number.numberBetween ''1000'',''2000''}',
    'fields.category_id.expression' = '#{number.numberBetween ''100'',''200''}',
    'fields.behavior.expression' = '#{Options.option ''PV'',''FAV'',''CART'',''BUY'')}',
    'fields.browser.expression' = '#{Options.option ''chrome'', ''firefox'', ''safari'')}',
    'fields.ts.expression' = '#{date.past ''5'',''1'',''SECONDS''}'
);