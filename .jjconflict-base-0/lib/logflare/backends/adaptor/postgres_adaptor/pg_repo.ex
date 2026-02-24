defmodule Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo do
  @moduledoc """
  Creates a Ecto.Repo for a source backend configuration, runs migrations and connects to it.

  Using the Source Backend source id we create a new Ecto.Repo which whom we will
  be able to connect to the configured PSQL URL, run migrations and insert data.
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgLogEvent
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Sources.Source

  require Logger

  @doc """
  Dynamically compiles a new Ecto.Repo module for a given source.
  Requires `:source` to be preloaded.
  """
  @spec create_repo(Backend.t()) :: atom()
  def create_repo(%Backend{} = backend) do
    {:ok, _} = SharedRepo.set_repo(backend)

    SharedRepo
  end

  @doc """
  Creates the Events table for the given source.
  """
  @spec create_events_table(Adaptor.source_backend()) ::
          :ok | {:error, :failed_migration}
  def create_events_table({source, backend}) do
    create_repo(backend)

    SharedRepo.with_repo(backend, fn ->
      SharedRepo.migrate!(source)
    end)
  end

  @doc """
  Returns the table name for a given Source.
  """
  @spec table_name(Source.t()) :: binary()
  def table_name(%Source{token: token}) do
    token
    |> Atom.to_string()
    |> String.replace("-", "_")
    |> then(&"log_events_#{&1}")
  end

  @doc """
  Drops the migration table.
  """
  @spec destroy_instance(Adaptor.source_backend(), timeout()) :: :ok
  def destroy_instance({source, backend}, timeout \\ 5000) do
    SharedRepo.with_repo(backend, fn ->
      do_destroy_instance(source, timeout)
    end)
  end

  defp do_destroy_instance(source, timeout) do
    if Process.whereis(SharedRepo) != nil do
      if Ecto.Adapters.SQL.table_exists?(SharedRepo, table_name(source)) do
        SharedRepo.down!(source)
      end

      SharedRepo.stop(timeout)
    end
  end

  @doc """
  Inserts a LogEvent into the given source backend table
  """
  @spec insert_log_event(Source.t(), Backend.t(), LogEvent.t()) :: {:ok, PgLogEvent.t()}
  def insert_log_event(source, backend, %LogEvent{} = le),
    do: insert_log_events(source, backend, [le])

  def insert_log_events(source, backend, events) when is_list(events) do
    table = PostgresAdaptor.table_name(source)

    schema = backend.config["schema"] || backend.config[:schema]

    event_params =
      Enum.map(events, fn log_event ->
        timestamp =
          log_event.body["timestamp"]
          |> DateTime.from_unix!(:microsecond)
          |> DateTime.to_naive()

        params = %{
          id: log_event.body["id"],
          event_message: log_event.body["event_message"],
          timestamp: timestamp,
          body: log_event.body
        }

        params
      end)

    SharedRepo.with_repo(backend, fn ->
      {count, _} = SharedRepo.insert_all(table, event_params, prefix: schema)
      {:ok, count}
    end)
  end
end
