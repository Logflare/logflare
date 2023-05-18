defmodule Logflare.Backends.Adaptor.Postgres.Repo do
  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.Logflare.Backends.Adaptor.Postgres.Supervisor
  require Logger

  @query "CREATE TABLE IF NOT EXISTS log_events (id TEXT PRIMARY KEY, event_message TEXT, metadata JSONB, timestamp TIMESTAMP);"
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

  def connect_to_source_backend(repository_module, %SourceBackend{config: config}, opts \\ []) do
    unless Process.whereis(repository_module) do
      %{url: url} = config
      opts = [{:url, url} | opts]

      {:ok, _} = DynamicSupervisor.start_child(Supervisor, repository_module.child_spec(opts))
    end

    :ok
  end

  def create_log_event_table(repository_module) do
    case Ecto.Adapters.SQL.query(repository_module, @query) do
      {:ok, _} ->
        :ok

      {:error, e} ->
        Logger.error(
          "Error creating table on target database for repository #{repository_module} with error #{inspect(e)}"
        )

        {:error, :database_error}
    end
  end
end
