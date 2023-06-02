defmodule Logflare.Backends.Adaptor.Postgres.Repo do
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.Adaptor.Postgres.Supervisor

  require Logger

  @ast (quote do
          use Ecto.Repo,
            otp_app: :logflare,
            adapter: Ecto.Adapters.Postgres
        end)

  @spec new_repository_for_source_backend(SourceBackend.t()) :: tuple()
  def new_repository_for_source_backend(source_backend) do
    name = Module.concat([Logflare.Repo.Postgres, "Adaptor#{source_backend.source_id}"])

    case Code.ensure_compiled(name) do
      {:module, _} -> nil
      _ -> {:module, _, _, _} = Module.create(name, @ast, Macro.Env.location(__ENV__))
    end

    name
  end

  @spec connect_to_source_backend(Ecto.Repo.t(), SourceBackend.t(), Keyword.t()) :: :ok
  def connect_to_source_backend(repository_module, %SourceBackend{config: config}, opts \\ []) do
    unless Process.whereis(repository_module) do
      %{url: url} = config
      opts = [{:url, url} | opts]

      {:ok, _} = DynamicSupervisor.start_child(Supervisor, repository_module.child_spec(opts))
    end

    :ok
  end

  @migrations [
    {20_230_602_173_023, Logflare.Backends.Adaptor.Postgres.Repo.Migrations.AddLogEvents}
  ]
  @spec create_log_event_table(Ecto.Repo.t()) :: :ok | {:error, :failed_migration}
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
