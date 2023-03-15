defmodule Logflare.Ecto.AfterConnect do
  require Logger
  alias Postgrex
  def run(conn) do
    Logger.debug("Running after connect actions")
    schema_name = Application.get_env(:logflare, :schema_name)
    Postgrex.query!(conn, "create schema if not exists $1",[schema_name])
    Postgrex.query!(conn, "SET search_path TO $1",[schema_name])
  end
end
