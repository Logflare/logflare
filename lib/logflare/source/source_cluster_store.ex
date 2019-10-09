defmodule Logflare.Sources.ClusterStore do
  alias Logflare.Redix, as: LogflareRedix
  alias LogflareRedix, as: LR
  alias Logflare.Source
  @default_ttl_sec 100

  def increment_counters(%Source{} = source) do
    suffix = gen_suffix()
    source_key = gen_source_log_count_key(source.token, suffix)
    user_key = gen_user_log_count_key(source.user.id, suffix)
    LR.increment(source_key, expire: @default_ttl_sec)
    LR.increment(user_key, expire: @default_ttl_sec)
    incr_total_log_count(source.token)
  end

  def get_user_log_counts(user_id) do
    unix_ts = unix_ts_now()

    keys =
      for i <- 0..@default_ttl_sec do
        gen_user_log_count_key(user_id, gen_suffix(unix_ts - i))
      end

    {:ok, result} = LR.multi_get(keys)

    result = clean_and_parse(result)

    {:ok, result}
  end

  def get_source_log_counts(source) do
    unix_ts = unix_ts_now()

    keys =
      for i <- 0..@default_ttl_sec do
        gen_source_log_count_key(source.token, gen_suffix(unix_ts - i))
      end

    {:ok, result} = LR.multi_get(keys)

    result = clean_and_parse(result)

    {:ok, result}
  end

  # Max rate

  def get_max_rate(source_id) do
    LR.get("source::#{source_id}::max_rate::global::v1")
  end

  def set_max_rate(source_id, value) do
    LR.set("source::#{source_id}::max_rate::global::v1", value, expire: @default_ttl_sec)
  end

  # Avg rate

  def set_avg_rate(source_id, value) do
    LR.set("source::#{source_id}::avg_rate::global::v1", value)
  end

  # Last rate

  def get_last_rate(source_id) do
    LR.get("source::#{source_id}::last_rate::global::v1")
  end

  def set_last_rate(source_id, value) do
    key = "source::#{source_id}::last_rate::global::v1"
    LR.set(key, value, expire: 3)
  end

  # Log count

  def incr_total_log_count(source_id) do
    key = "source::#{source_id}::total_log_count::v1"
    LR.increment(key)
  end

  def set_total_log_count(source_id, value) do
    key = "source::#{source_id}::total_log_count::v1"
    LR.set(key, value, expire: 86_400)
  end

  def get_total_log_count(source_id) do
    LR.get("source::#{source_id}::total_log_count::v1")
  end

  # Buffer counts

  def set_buffer_count(source_id, value) do
    key = "source::#{source_id}::buffer::#{Node.self()}::v1"
    LR.set(key, value, expire: 5)
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

  defp clean_and_parse(result) do
    result
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&String.to_integer/1)
  end

  # Name generators
  defp gen_source_log_count_key(source_id, suffix) do
    "source::#{source_id}::log_count::#{suffix}::v1"
  end

  defp gen_user_log_count_key(user_id, suffix) do
    "user::#{user_id}::log_count::#{suffix}::v1"
  end

  defp gen_suffix(ts \\ unix_ts_now()) do
    "#{timestamp_suffix()}:#{ts}"
  end

  defp unix_ts_now() do
    NaiveDateTime.utc_now() |> Timex.to_unix()
  end

  defp timestamp_suffix() do
    "timestamp"
  end
end
