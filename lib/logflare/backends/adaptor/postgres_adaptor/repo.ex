defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Repo do
  @moduledoc """
  Creates a Ecto.Repo for a source backend configuration, runs migrations and connects to it.

  Using the Source Backend source id we create a new Ecto.Repo which whom we will
  be able to connect to the configured PSQL URL, run migrations and insert data.
  """
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Supervisor

  require Logger

  @ast (quote do
          use Ecto.Repo,
            otp_app: :logflare,
            adapter: Ecto.Adapters.Postgres
        end)

  @spec new_repository_for_source_backend(SourceBackend.t()) :: atom()
  def new_repository_for_source_backend(source_backend) do
    name = Module.concat([Logflare.Repo.Postgres, "Adaptor#{source_backend.source_id}"])

    case Code.ensure_compiled(name) do
      {:module, _} -> nil
      _ -> {:module, _, _, _} = Module.create(name, @ast, Macro.Env.location(__ENV__))
    end

    migration_table = "schema_migrations_#{source_backend.source_id}"
    Application.put_env(:logflare, name, migration_source: migration_table)

    name
  end

  @spec connect_to_source_backend(Ecto.Repo.t(), SourceBackend.t(), Keyword.t()) :: :ok
  def connect_to_source_backend(repository_module, %SourceBackend{config: config}, opts \\ []) do
    unless Process.whereis(repository_module) do
      opts = [{:url, config.url} | opts]

      {:ok, _} = DynamicSupervisor.start_child(Supervisor, repository_module.child_spec(opts))
    end

    :ok
  end

  @migrations [
    {1, Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations.AddLogEvents}
  ]
  @spec create_log_event_table(Ecto.Repo.t(), []) ::
          :ok | {:error, :failed_migration}
  def create_log_event_table(repository_module, migrations \\ @migrations) do
    Ecto.Migrator.run(repository_module, migrations, :up, all: true)

    :ok
  rescue
    e in Postgrex.Error ->
      Logger.error("Error creating log_events table: #{inspect(e)}")
      {:error, :failed_migration}
  end

  def migrations, do: @migrations
end
