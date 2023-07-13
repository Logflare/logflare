defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Repo do
  @moduledoc """
  Creates a Ecto.Repo for a source backend configuration, runs migrations and connects to it.

  Using the Source Backend source id we create a new Ecto.Repo which whom we will
  be able to connect to the configured PSQL URL, run migrations and insert data.
  """
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations.AddLogEvents
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Supervisor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.SourceBackend
  alias Logflare.Repo
  alias Logflare.Source

  require Logger

  @ast (quote do
          use Ecto.Repo,
            otp_app: :logflare,
            adapter: Ecto.Adapters.Postgres
        end)

  @spec create_repo(SourceBackend.t()) :: atom()
  def create_repo(source_backend) do
    source_backend = Repo.preload(source_backend, :source)
    name = get_repo_module(source_backend)

    case Code.ensure_compiled(name) do
      {:module, _} -> nil
      _ -> {:module, _, _, _} = Module.create(name, @ast, Macro.Env.location(__ENV__))
    end

    migration_table = PostgresAdaptor.migrations_table_name(source_backend)
    Application.put_env(:logflare, name, migration_source: migration_table)

    name
  end

  @doc """
  Retrieves the repo module. Requires `:source` to be preloaded.
  """
  @spec get_repo_module(SourceBackend.t()) :: Ecto.Repo.t()
  def get_repo_module(%SourceBackend{source: %Source{token: token}} = source_backend) do
    Module.concat([Logflare.Repo.Postgres, "Adaptor#{token}"])
  end

  @doc """
  Connects to a given postgres. Requires `:source` to be preloaded.
  """
  @spec connect_to_repo(SourceBackend.t(), Keyword.t()) :: :ok
  def connect_to_repo(%SourceBackend{config: config} = source_backend, opts \\ []) do
    repo = get_repo_module(source_backend)

    unless Process.whereis(repo) do
      pool_size =
        Keyword.get(Application.get_env(:logflare, :postgres_backend_adapter), :pool_size, 10)

      # use same pool type as Logflare.Repo
      pool = Keyword.get(Application.get_env(:logflare, Logflare.Repo), :pool)

      opts = [
        {:url, config["url"] || config.url},
        {:name, repo},
        {:pool, pool},
        {:pool_size, pool_size} | opts
      ]

      {:ok, _} = DynamicSupervisor.start_child(Supervisor, repo.child_spec(opts))
    end

    :ok
  end

  @spec create_log_events_table(SourceBackend.t(), list() | nil) ::
          :ok | {:error, :failed_migration}
  def create_log_events_table(source_backend, override_migrations \\ nil) do
    repository_module = get_repo_module(source_backend)
    migrations = if override_migrations, do: override_migrations, else: migrations(source_backend)
    Ecto.Migrator.run(repository_module, migrations, :up, all: true)

    :ok
  rescue
    e in Postgrex.Error ->
      Logger.error("Error creating log_events table: #{inspect(e)}")
      {:error, :failed_migration}
  end

  @spec table_name(SourceBackend.t() | Source.t()) :: binary()
  def table_name(%SourceBackend{} = source_backend) do
    %{source: source} = Repo.preload(source_backend, :source)
    table_name(source)
  end

  def table_name(%Source{token: token}) do
    token
    |> Atom.to_string()
    |> String.replace("-", "_")
    |> then(&"log_events_#{&1}")
  end

  @spec migrations(SourceBackend.t()) :: list({pos_integer(), atom()})
  def migrations(source_backend), do: [{1, AddLogEvents.generate_migration(source_backend)}]
end
