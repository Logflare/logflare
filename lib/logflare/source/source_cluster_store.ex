defmodule Logflare.Sources.ClusterStore do
  alias Logflare.Redix, as: LogflareRedix
  alias LogflareRedix, as: LR
  alias Logflare.Source
  alias Timex.Duration
  @default_ttl_sec 100
  @hourly_counter_expiration_ttl_sec Duration.from_hours(24) |> Duration.to_seconds()

  def increment_counters(%Source{} = source) do
    source.token
    |> source_log_count_key(:second, Timex.now())
    |> LR.increment(expire: @default_ttl_sec)

    source.user.id
    |> user_log_count_key(:second, Timex.now())
    |> LR.increment(expire: @default_ttl_sec)

    source.token
    |> source_log_count_key(:hour, Timex.now())
    |> LR.increment(expire: @hourly_counter_expiration_ttl_sec)

    incr_total_log_count(source.token)
  end

  def get_user_log_counts(user_id) do
    unix_ts = unix_ts_now()

    keys =
      for i <- 0..@default_ttl_sec do
        user_log_count_key(user_id, :second, unix_ts - i)
      end

    {:ok, result} = LR.multi_get(keys)

    result = clean_and_parse(result)

    {:ok, result}
  end

  def get_source_log_counts(source) do
    unix_ts = unix_ts_now()

    keys =
      for i <- 0..@default_ttl_sec do
        source_log_count_key(source.token, :second, unix_ts - i)
      end

    {:ok, result} = LR.multi_get(keys)

    result = clean_and_parse(result)

    {:ok, result}
  end

  # Max rate

  def get_max_rate(source_id) do
    LR.get("source::#{source_id}::max_rate::global::v1")
  end

  @expire 86_400
  def set_max_rate(source_id, value) do
    LR.set("source::#{source_id}::max_rate::global::v1", value, expire: @expire)
  end

  # Avg rate

  def set_avg_rate(source_id, value) do
    LR.set("source::#{source_id}::avg_rate::global::v1", value)
  end

  def get_avg_rate(source_id) do
    periods = 24

    keys =
      for i <- 0..periods do
        source_log_count_key(source_id, :hour, Timex.shift(Timex.now(), hours: -i))
      end

    {:ok, result} = LR.multi_get(keys)

    result = clean_and_parse(result)

    rate = Enum.sum(result) / Duration.to_seconds(Duration.from_hours(length(result)))

    {:ok, rate}
  end

  # Last rate

  def get_last_rate(source_id) do
    LR.get("source::#{source_id}::last_rate::global::v1")
  end

  def set_last_rate(source_id, value) do
    key = "source::#{source_id}::last_rate::global::v1"
    LR.set(key, value, expire: 3)
  end

  # Total log count

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
  defp source_log_count_key(source_id, granularity, ts) when is_atom(granularity) do
    suffix = gen_suffix(granularity, ts)
    "source::#{source_id}::log_count::#{suffix}::v1"
  end

  defp user_log_count_key(user_id, granularity, ts) when is_atom(granularity) do
    suffix = gen_suffix(granularity, ts)
    "user::#{user_id}::log_count::#{suffix}::v1"
  end

  defp gen_suffix(granularity, ts) do
    ts =
      if is_integer(ts) do
        Timex.from_unix(ts)
      else
        ts
      end

    fmtstr =
      case granularity do
        :second -> "{Mshort}-{0D}-{h24}-{m}-{s}"
        :minute -> "{Mshort}-{0D}-{h24}-{m}-00"
        :hour -> "{Mshort}-{0D}-{h24}-00-00"
      end

    tsfmt = Timex.format!(ts, fmtstr)
    "timestamp::#{granularity}::#{tsfmt}"
  end

  defp unix_ts_now() do
    NaiveDateTime.utc_now() |> Timex.to_unix()
  end
end
