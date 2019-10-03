defmodule Logflare.Source.Store do
  @default_conn :redix
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
end
