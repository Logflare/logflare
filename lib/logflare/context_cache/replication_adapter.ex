defmodule Logflare.ContextCache.ReplicationAdapter do
  @moduledoc """
  Cainophile adapter that streams publications over a temporary replication slot.

  Unlike Cainophile's default adapter, it starts a `Logflare.ContextCache.FlushConfirmer`
  linked to the publisher, so the slot keeps advancing while the published tables are idle
  instead of retaining WAL for the whole cluster. Only temporary slots are supported.
  """

  @behaviour Cainophile.Adapters.Postgres.AdapterBehaviour

  alias Cainophile.Adapters.Postgres.EpgsqlImplementation
  alias Cainophile.Adapters.Postgres.State
  alias Logflare.ContextCache.FlushConfirmer

  @impl true
  def init(config) do
    epgsql = Keyword.fetch!(config, :epgsql)

    with {:ok, connection} <- :epgsql.connect(Map.put(epgsql, :replication, ~c"database")),
         {:ok, slot_name} <- create_slot(connection, Keyword.fetch!(config, :slot)),
         :ok <-
           :epgsql.start_replication(
             connection,
             String.to_charlist(slot_name),
             self(),
             [],
             wal_position(config),
             plugin_options(config)
           ),
         {:ok, _confirmer} <-
           FlushConfirmer.start_link(
             replication: connection,
             epgsql: epgsql,
             interval: Keyword.fetch!(config, :flush_confirmation_interval)
           ) do
      {:ok, %State{config: config, connection: connection}}
    else
      {:error, reason} -> {:stop, inspect(reason)}
    end
  end

  @impl true
  defdelegate acknowledge_lsn(connection, lsn), to: EpgsqlImplementation

  @impl true
  defdelegate cleanup(connection), to: EpgsqlImplementation

  defp create_slot(connection, :temporary) do
    suffix = 5 |> :crypto.strong_rand_bytes() |> Base.encode32(case: :lower, padding: false)
    slot_name = "cainophile_" <> suffix

    case :epgsql.squery(
           connection,
           "CREATE_REPLICATION_SLOT #{slot_name} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"
         ) do
      {:ok, _columns, _rows} -> {:ok, slot_name}
      {:error, _reason} = error -> error
    end
  end

  defp wal_position(config) do
    {xlog, offset} = Keyword.get(config, :wal_position, {"0", "0"})
    String.to_charlist("#{xlog}/#{offset}")
  end

  defp plugin_options(config) do
    publication_names =
      config
      |> Keyword.fetch!(:publications)
      |> Enum.map_join(",", &~s("#{&1}"))

    ~c"proto_version '1', publication_names '#{publication_names}'"
  end
end
