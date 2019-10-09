defmodule Logflare.Sources.ClusterStore do
  alias Logflare.Redix, as: LogflareRedix
  alias LogflareRedix, as: LR
  alias Logflare.Source
  alias Timex.Duration

  @second_counter_expiration_sec 60
  @hour_counter_expiration_sec Duration.from_hours(24) |> Duration.to_seconds() |> round()
  @minute_counter_expiration_sec Duration.from_minutes(10) |> Duration.to_seconds() |> round()

  def increment_counters(%Source{} = source) do
    source.token
    |> source_log_count_key(:hour, Timex.now())
    |> LR.increment(expire: @hour_counter_expiration_sec)

    {:ok, last_second_source_counter} =
      source.token
      |> source_log_count_key(:second, Timex.now())
      |> LR.increment(expire: @second_counter_expiration_sec)

    set_source_last_rate(source.token, last_second_source_counter, period: :second)

    {:ok, last_second_user_counter} =
      source.user.id
      |> user_log_count_key(:second, Timex.now())
      |> LR.increment(expire: @second_counter_expiration_sec)

    set_user_last_rate(source.token, last_second_user_counter, period: :second)

    {:ok, last_minute_source_counter} =
      source.token
      |> source_log_count_key(:minute, Timex.now())
      |> LR.increment(expire: @minute_counter_expiration_sec)

    set_source_last_rate(source.token, last_minute_source_counter, period: :minute)

    {:ok, last_minute_user_counter} =
      source.token
      |> source_log_count_key(:minute, Timex.now())
      |> LR.increment(expire: @minute_counter_expiration_sec)

    set_user_last_rate(source.token, last_minute_user_counter, period: :minute)

    incr_total_log_count(source.token)
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

    period_seconds =
      result
      |> length()
      |> Duration.from_hours()
      |> Duration.to_seconds()

    rps =
      if period_seconds == 0 do
        0
      else
        Enum.sum(result) / period_seconds
      end

    {:ok, rps}
  end

  # Last rate

  def get_source_last_rate(source_id, period: period) do
    LR.get("source::#{source_id}::last_rate::#{period}::global::v1")
  end

  def set_source_last_rate(source_id, value, period: period) do
    key = "source::#{source_id}::last_rate::#{period}::global::v1"
    LR.set(key, value, expire: 3)
  end

  def get_user_last_rate(user_id, period: period) do
    LR.get("user::#{user_id}::last_rate::#{period}::global::v1")
  end

  def set_user_last_rate(user_id, value, period: period) do
    key = "user::#{user_id}::last_rate::#{period}::global::v1"
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
