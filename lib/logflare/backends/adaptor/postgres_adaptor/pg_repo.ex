defmodule Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo do
  @moduledoc """
  Creates a Ecto.Repo for a source backend configuration, runs migrations and connects to it.

  Using the Source Backend source id we create a new Ecto.Repo which whom we will
  be able to connect to the configured PSQL URL, run migrations and insert data.
  """
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations.AddLogEvents
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Supervisor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgLogEvent
  alias Logflare.Backends.SourceBackend
  alias Logflare.Source
  alias Logflare.LogEvent

  require Logger

  @ast (quote do
          use Ecto.Repo,
            otp_app: :logflare,
            adapter: Ecto.Adapters.Postgres
        end)

  @doc """
  Dynamically compiles a new Ecto.Repo module for a given source.
  Requires `:source` to be preloaded.
  """
  @spec create_repo(SourceBackend.t()) :: atom()

  def create_repo(%SourceBackend{source: %Source{}} = source_backend) do
    name = get_repo_module(source_backend)

    case Code.ensure_compiled(name) do
      {:module, _} -> nil
      _ -> {:module, _, _, _} = Module.create(name, @ast, Macro.Env.location(__ENV__))
    end

    migration_table = migrations_table_name(source_backend)

    schema = Map.get(source_backend.config, "schema")

    after_connect =
      if schema do
        {Postgrex, :query!, ["set search_path=#{schema}", []]}
      end

    Application.put_env(:logflare, name,
      migration_source: migration_table,
      after_connect: after_connect
    )

    name
  end

  @doc """
  Retrieves the repo module. Requires `:source` to be preloaded.
  """
  @spec get_repo_module(SourceBackend.t()) :: Ecto.Repo.t()
  def get_repo_module(%SourceBackend{source: %Source{token: token}}) do
    token = token |> Atom.to_string() |> String.replace("-", "")
    Module.concat([Logflare.Repo.Postgres, "Adaptor#{token}"])
  end

  @doc """
  Connects to a given postgres. Requires `:source` to be preloaded.
  """
  @spec connect_to_repo(SourceBackend.t()) :: :ok
  def connect_to_repo(%SourceBackend{config: config} = source_backend) do
    repo = get_repo_module(source_backend)

    unless Process.whereis(repo) do
      pool_size =
        Keyword.get(Application.get_env(:logflare, :postgres_backend_adapter), :pool_size, 10)

      opts = [
        {:url, config["url"] || config.url},
        {:name, repo},
        {:pool_size, pool_size}
      ]

      {:ok, _} = DynamicSupervisor.start_child(Supervisor, repo.child_spec(opts))
    end

    :ok
  end

  @doc """
  Creates the Log Events table for the given source.
  """
  @spec create_log_events_table(SourceBackend.t(), list() | nil) ::
          :ok | {:error, :failed_migration}
  def create_log_events_table(source_backend, override_migrations \\ nil) do
    repository_module = get_repo_module(source_backend)
    migrations = if override_migrations, do: override_migrations, else: migrations(source_backend)
    schema = Map.get(source_backend.config, "schema") || Map.get(source_backend.config, :schema)

    prefix =
      case schema do
        nil ->
          []

        schema ->
          query = """
          CREATE SCHEMA IF NOT EXISTS #{schema}
          """

          {:ok, _} = Ecto.Adapters.SQL.query(repository_module, query, [])

          [prefix: schema]
      end

    opts = [all: true] ++ prefix
    Ecto.Migrator.run(repository_module, migrations, :up, opts)

    :ok
  rescue
    e in Postgrex.Error ->
      Logger.error("Error creating log_events table: #{inspect(e)}")
      {:error, :failed_migration}
  end

  @doc """
  Returns the table name for a given Source or SourceBackend.
  If SourceBackend, :source must be preloaded.
  """
  @spec table_name(SourceBackend.t() | Source.t()) :: binary()
  def table_name(%SourceBackend{source: %_{} = source}), do: table_name(source)

  def table_name(%Source{token: token}) do
    token
    |> Atom.to_string()
    |> String.replace("-", "_")
    |> then(&"log_events_#{&1}")
  end

  @doc """
  Retunrns a list of migrations to run.
  """
  @spec migrations(SourceBackend.t()) :: list({pos_integer(), atom()})
  def migrations(source_backend), do: [{1, AddLogEvents.generate_migration(source_backend)}]

  @doc """
  Rolls back all migrations
  """
  @spec rollback_migrations(SourceBackend.t()) :: :ok
  def rollback_migrations(source_backend) do
    repository_module = create_repo(source_backend)
    Ecto.Migrator.run(repository_module, migrations(source_backend), :down, all: true)

    :ok
  end

  @doc """
  Drops the migration table
  """
  @spec drop_migrations_table(SourceBackend.t()) :: :ok
  def drop_migrations_table(source_backend) do
    repository_module = create_repo(source_backend)
    migrations_table = migrations_table_name(source_backend)
    Ecto.Adapters.SQL.query!(repository_module, "DROP TABLE IF EXISTS #{migrations_table}")
    :ok
  end

  @doc """
  Returns the migrations table name used for a given source
  """
  @spec migrations_table_name(SourceBackend.t()) :: String.t()
  def migrations_table_name(%SourceBackend{source: %Source{token: token}}) do
    token = token |> Atom.to_string() |> String.replace("-", "_")
    "schema_migrations_#{token}"
  end

  @doc """
  Inserts a LogEvent into the given source backend table
  """
  @spec insert_log_event(SourceBackend.t(), LogEvent.t()) :: {:ok, PgLogEvent.t()}
  def insert_log_event(%{config: config} = source_backend, %LogEvent{} = log_event) do
    repo = get_repo_module(source_backend)
    table = PostgresAdaptor.table_name(source_backend)

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

    changeset =
      %PgLogEvent{}
      |> Ecto.put_meta(source: table, prefix: config["schema"])
      |> PgLogEvent.changeset(params)

    repo.insert(changeset)
  end
end
