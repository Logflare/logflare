defmodule Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo do
  @moduledoc """
  Creates a Ecto.Repo for a source backend configuration, runs migrations and connects to it.

  Using the Source Backend source id we create a new Ecto.Repo which whom we will
  be able to connect to the configured PSQL URL, run migrations and insert data.
  """

  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgLogEvent
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Source
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
  Retrieves the repo module. Requires `:source` to be preloaded.
  """
  @spec get_repo_module(Backend.t()) :: Ecto.Repo.t()
  def get_repo_module(%Backend{config: config}) do
    data = inspect(config)
    sha256 = :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)
    Module.concat([Logflare.Repo.Postgres, "Adaptor#{sha256}"])
  end

  @doc """
  Creates the Log Events table for the given source.
  """
  @spec create_log_events_table(Adaptor.source_backend()) ::
          :ok | {:error, :failed_migration}
  def create_log_events_table({source, backend}) do
    mod = create_repo(backend)

    mod.migrate!(source)
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
  Drops the migration table
  """
  @spec destroy_instance(Adaptor.source_backend(), timeout()) :: :ok
  def destroy_instance({source, backend}, timeout \\ 5000) do
    SharedRepo.with_repo(backend, fn ->
      SharedRepo.down!(source)
      SharedRepo.stop(timeout)
    end)
  end

  @doc """
  Inserts a LogEvent into the given source backend table
  """
  @spec insert_log_event(Backend.t(), LogEvent.t()) :: {:ok, PgLogEvent.t()}
  def insert_log_event(backend, %LogEvent{} = log_event) do
    table = PostgresAdaptor.table_name(log_event.source)

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

    schema = backend.config["schema"] || backend.config[:schema]

    changeset =
      %PgLogEvent{}
      |> Ecto.put_meta(source: table, prefix: schema)
      |> PgLogEvent.changeset(params)

    SharedRepo.with_repo(backend, fn ->
      SharedRepo.insert(changeset)
    end)
  end
end
