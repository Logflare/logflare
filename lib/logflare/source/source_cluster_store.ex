defmodule Logflare.Sources.ClusterStore do
  @default_conn :logflare_redix
  alias Logflare.Redix, as: LogflareRedix
  alias LogflareRedix, as: LR
  alias Logflare.Source
  @default_ttl_sec 300

  def increment(key) do
    Redix.command(@default_conn, ["INCR", key])
  end

  def get(key) do
    Redix.command(@default_conn, ["GET", key])
  end

  def set(key, value) do
    LogflareRedix.command(["SET", key, value, "EX", @default_ttl_sec])
  end

  def set_max_rate(source_id, value) do
    set("source::#{source_id}::max_rate::global::v1", value)
  end

  def get_user_log_counts(user_id) do
    unix_ts = unix_ts_now()

    keys =
      for i <- 0..@default_ttl_sec do
        gen_user_log_count_key(user_id, gen_suffix(unix_ts - i))
      end

    {:ok, result} = LogflareRedix.command(["MGET" | keys])

    result =
      result
      # TODO
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&String.to_integer/1)

    {:ok, result}
  end

  def get_source_log_counts(source) do
    unix_ts = unix_ts_now()

    keys =
      for i <- 0..@default_ttl_sec do
        gen_source_log_count_key(source.token, gen_suffix(unix_ts - i))
      end

    {:ok, result} = LogflareRedix.command(["MGET" | keys])

    result =
      result
      # TODO
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&String.to_integer/1)

    {:ok, result}
  end

  def clean_and_parse(result) do
    result
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&String.to_integer/1)
  end

  # Max rate
  def get_max_rate(source_id) do
    get("source::#{source_id}::max_rate::global::v1")
  end

  # Avg rate
  def set_avg_rate(source_id, value) do
    set("source::#{source_id}::avg_rate::global::v1", value)
  end

  # Last rate
  def get_last_rate(source_id) do
    get("source::#{source_id}::last_rate::global::v1")
  end

  def set_last_rate(source_id, value) do
    key = "source::#{source_id}::last_rate::global::v1"
    LogflareRedix.command(["SET", key, value, "EX", 1])
  end

  # Log count
  def set_total_log_count(source_id, value) do
    key = "source::#{source_id}::total_log_count::#{Node.self()}::v1"
    LR.command(["SET", key, value, "EX", 300])
  end

  def get_total_log_count(source_id) do
    with {:ok, keys} <- LR.scan_all_match("*source::#{source_id}::total_log_count::*"),
         {:ok, result} <- LR.multi_get(keys) do
      values = clean_and_parse(result)
      total_log_count = Enum.max(values)
      {:ok, total_log_count}
    else
      {:error, :empty_keys_list} -> {:ok, 0}
      errtup -> errtup
    end
  end

  def set_buffer_count(source_id, value) do
    key = "source::#{source_id}::buffer::#{Node.self()}::v1"
    LR.command(["SET", key, value, "EX", 5])
  end

  def get_buffer_count(source_id) do
    with {:ok, keys} <- LR.scan_all_match("*source::#{source_id}::buffer::*"),
         {:ok, result} <- LR.multi_get(keys) do
      values = clean_and_parse(result)
      buffer = Enum.sum(values)
      {:ok, buffer}
    else
      {:error, :empty_keys_list} -> {:ok, 0}
      errtup -> errtup
    end
  end

  def reset(key) when is_atom(key) do
    LogflareRedix.command(["SET", key, "0"])
  end

  def increment_counters(%Source{} = source) do
    suffix = gen_suffix()
    source_key = gen_source_log_count_key(source.token, suffix)
    user_key = gen_user_log_count_key(source.user.id, suffix)
    incr_and_expire(source_key)
    incr_and_expire(user_key)
  end

  def incr_and_expire(key) do
    {:ok, [val, _]} =
      Redix.pipeline(@default_conn, [["INCR", key], ["EXPIRE", key, @default_ttl_sec]])

    val
  end

  # Name generators
  def gen_source_log_count_key(source_id, suffix) do
    "source::#{source_id}::log_count::#{suffix}::v1"
  end

  def gen_user_log_count_key(user_id, suffix) do
    "user::#{user_id}::log_count::#{suffix}::v1"
  end

  def gen_suffix(ts \\ unix_ts_now()) do
    "#{timestamp_suffix()}:#{ts}"
  end

  def unix_ts_now() do
    NaiveDateTime.utc_now() |> Timex.to_unix()
  end

  def timestamp_suffix() do
    "timestamp"
  end
end
