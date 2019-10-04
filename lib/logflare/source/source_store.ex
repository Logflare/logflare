defmodule Logflare.Sources.Store do
  @default_conn :redix
  alias Logflare.Source

  def increment(key) when is_atom(key) do
    Redix.command(@default_conn, ["SETNX", key, "0"])
    Redix.command(@default_conn, ["INCR", key])
  end

  def get(key) when is_atom(key) do
    Redix.command(@default_conn, ["GET", key])
  end

  def reset(key) when is_atom(key) do
    Redix.command(@default_conn, ["SET", key, "0"])
  end

  def increment_and_get_rates(%Source{} = source) do
    {:ok, minute} = NaiveDateTime.utc_now() |> Timex.format("{m}")
    suffix = "#{minute}:minute"
    source_key = "#{gen_source_rate_key(source.token)}:#{suffix}"
    user_key = "#{gen_user_rate_key(source.user.id)}:#{suffix}"
    current_user_rate = incr_user_log_count(user_key)
    current_source_rate = incr_source_log_count(source_key)
    %{user_rate: current_user_rate, source_rate: current_source_rate}
  end

  def incr_source_log_count(source_id) do
    incr_and_expire(source_id)
  end

  def incr_user_log_count(user_id) do
    incr_and_expire(user_id)
  end

  def incr_and_expire(key) do
    {:ok, [val, _]} = Redix.pipeline(@default_conn, [["INCR", key], ["EXPIRE", key, 300]])
    val
  end

  def gen_source_rate_key(source_id) when is_atom(source_id) do
    "#{source_id}_source_rate"
  end

  def gen_user_rate_key(id) when is_integer(id) do
    "#{id}_user_rate"
  end
end
