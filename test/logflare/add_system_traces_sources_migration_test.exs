defmodule Logflare.AddSystemTracesSourcesMigrationTest do
  use Logflare.DataCase, async: false

  alias Logflare.Repo
  alias Logflare.Repo.Migrations.AddSystemTracesSources
  alias Logflare.Sources
  alias Logflare.Sources.Source

  @migration Application.app_dir(
               :logflare,
               "priv/repo/migrations/20261118000000_add_system_traces_sources.exs"
             )
  @ignored_fields [:id, :token, :user_id, :inserted_at, :updated_at]

  setup do
    unless Code.ensure_loaded?(AddSystemTracesSources), do: Code.require_file(@migration)
    insert(:plan)
    :ok
  end

  describe "up/0" do
    test "adds system.traces matching the app-created source for users with monitoring on" do
      monitored = insert(:user, system_monitoring: true)
      unmonitored = insert(:user, system_monitoring: false)
      logs = insert(:source, user: monitored, system_source: true, system_source_type: :logs)

      reference =
        insert(:user)
        |> Map.fetch!(:id)
        |> Sources.create_user_system_sources()
        |> Enum.find(&(&1.system_source_type == :traces))

      migrate(:up)

      assert [traces] = list_traces_sources(monitored.id)
      assert list_traces_sources(unmonitored.id) == []
      assert Repo.reload!(logs).system_source_type == :logs

      assert comparable_fields(traces) == comparable_fields(reference)
      assert is_atom(traces.token)
    end

    test "keeps an existing system.traces source" do
      user = insert(:user, system_monitoring: true)
      Sources.create_user_system_sources(user.id)
      [existing] = list_traces_sources(user.id)

      migrate(:up)

      assert [%Source{id: id}] = list_traces_sources(user.id)
      assert id == existing.id
    end
  end

  describe "down/0" do
    test "removes system.traces sources and their backend links, keeping other sources" do
      user = insert(:user, system_monitoring: true)
      sources = Sources.create_user_system_sources(user.id)
      traces = Enum.find(sources, &(&1.system_source_type == :traces))
      insert(:backend, user: user, sources: [traces])

      migrate(:down)

      assert list_traces_sources(user.id) == []

      assert user.id
             |> Sources.list_system_sources_by_user()
             |> Enum.map(& &1.system_source_type)
             |> Enum.sort() == [:logs, :metrics]
    end
  end

  @spec migrate(:up | :down) :: :ok
  defp migrate(:up) do
    migrate_up()
    :ok
  end

  defp migrate(:down) do
    version = migrate_up()

    :ok =
      Ecto.Migrator.down(Repo, version, AddSystemTracesSources, log: false, migration_lock: false)
  end

  @spec migrate_up() :: pos_integer()
  defp migrate_up do
    version = 9_000_000_000_000_000 + System.unique_integer([:positive])

    :ok =
      Ecto.Migrator.up(Repo, version, AddSystemTracesSources, log: false, migration_lock: false)

    version
  end

  @spec list_traces_sources(pos_integer()) :: [Source.t()]
  defp list_traces_sources(user_id) do
    Repo.all(from(s in Source, where: s.user_id == ^user_id and s.system_source_type == :traces))
  end

  @spec comparable_fields(Source.t()) :: map()
  defp comparable_fields(source) do
    Map.take(source, Source.__schema__(:fields) -- @ignored_fields)
  end
end
