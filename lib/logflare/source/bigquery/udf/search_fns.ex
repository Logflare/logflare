defmodule Logflare.User.BigQueryUDFs.SearchFns do
  @moduledoc false

  def lf_timestamp_sub(bq_dataset_id) do
    "
    CREATE OR REPLACE FUNCTION
  `#{bq_dataset_id}`.LF_TIMESTAMP_SUB(_timestamp TIMESTAMP,
    _interval INT64,
    _date_part STRING) AS (
    CASE _date_part
      WHEN 'MICROSECOND' THEN TIMESTAMP_SUB(_timestamp, INTERVAL _interval MICROSECOND)
      WHEN 'MILLISECOND' THEN TIMESTAMP_SUB(_timestamp, INTERVAL _interval MILLISECOND)
      WHEN 'SECOND' THEN TIMESTAMP_SUB(_timestamp, INTERVAL _interval SECOND)
      WHEN 'MINUTE' THEN TIMESTAMP_SUB(_timestamp, INTERVAL _interval MINUTE)
      WHEN 'HOUR' THEN TIMESTAMP_SUB(_timestamp, INTERVAL _interval HOUR)
       WHEN 'DAY' THEN TIMESTAMP_SUB(_timestamp, INTERVAL _interval DAY)
  END
    );
  "
  end

  def lf_timestamp_trunc(bq_dataset_id) do
    "
    CREATE OR REPLACE FUNCTION
    `#{bq_dataset_id}`.LF_TIMESTAMP_TRUNC(_timestamp TIMESTAMP, _date_part STRING) AS (
        CASE _date_part
        WHEN 'MICROSECOND' THEN TIMESTAMP_TRUNC(_timestamp, MICROSECOND)
        WHEN 'MILLISECOND' THEN TIMESTAMP_TRUNC(_timestamp, MILLISECOND)
        WHEN 'SECOND' THEN TIMESTAMP_TRUNC(_timestamp, SECOND)
        WHEN 'MINUTE' THEN TIMESTAMP_TRUNC(_timestamp,  MINUTE)
        WHEN 'HOUR' THEN TIMESTAMP_TRUNC(_timestamp, HOUR)
        WHEN 'DAY' THEN TIMESTAMP_TRUNC(_timestamp, DAY)
  END);
  "
  end

  def lf_generate_timestamp_array(bq_dataset_id) do
    "
    CREATE OR REPLACE FUNCTION
    `#{bq_dataset_id}`.LF_GENERATE_TIMESTAMP_ARRAY(_timestamp1 TIMESTAMP, _timestamp2 TIMESTAMP, _interval INT64, _date_part STRING) AS (
        CASE _date_part
        WHEN 'MICROSECOND' THEN GENERATE_TIMESTAMP_ARRAY(_timestamp1, _timestamp2, INTERVAL _interval MICROSECOND)
        WHEN 'MILLISECOND' THEN GENERATE_TIMESTAMP_ARRAY(_timestamp1, _timestamp2, INTERVAL _interval MILLISECOND)
        WHEN 'SECOND' THEN GENERATE_TIMESTAMP_ARRAY(_timestamp1, _timestamp2, INTERVAL _interval SECOND)
        WHEN 'MINUTE' THEN GENERATE_TIMESTAMP_ARRAY(_timestamp1, _timestamp2, INTERVAL _interval MINUTE)
        WHEN 'HOUR' THEN GENERATE_TIMESTAMP_ARRAY(_timestamp1, _timestamp2, INTERVAL _interval HOUR)
        WHEN 'DAY' THEN GENERATE_TIMESTAMP_ARRAY(_timestamp1, _timestamp2, INTERVAL _interval DAY)
  END);
  "
  end
end
