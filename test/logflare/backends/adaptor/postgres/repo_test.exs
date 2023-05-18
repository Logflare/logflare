defmodule Logflare.Backends.Adaptor.Postgres.RepoTest do
  use Logflare.DataCase, async: false
  alias Logflare.Backends.Adaptor.Postgres.Repo

  setup do
    %{username: username, password: password, database: database, hostname: hostname} =
      Application.get_env(:logflare, Logflare.Repo) |> Map.new()

    url = "postgresql://#{username}:#{password}@#{hostname}/#{database}"

    source = insert(:source, user: insert(:user))
    source_backend = insert(:source_backend, type: :postgres, config: %{url: url}, source: source)

    %{source_backend: source_backend}
  end

  describe "new_repository_for_source_backend/1" do
    test "creates a new Ecto.Repo for given source_backend", %{source_backend: source_backend} do
      repository = Repo.new_repository_for_source_backend(source_backend)
      assert Keyword.get(repository.__info__(:attributes), :behaviour) == [Ecto.Repo]
    end

    test "name of the module uses source_id", %{source_backend: source_backend} do
      repository = Repo.new_repository_for_source_backend(source_backend)

      assert repository ==
               Module.concat([Logflare.Repo.Postgres, "Adaptor#{source_backend.source_id}"])
    end
  end

  describe "create_log_event_table/1" do
    setup %{source_backend: source_backend} do
      repository = Repo.new_repository_for_source_backend(source_backend)
      Repo.connect_to_source_backend(repository, source_backend, pool: Ecto.Adapters.SQL.Sandbox)
      Ecto.Adapters.SQL.Sandbox.mode(repository, :auto)

      on_exit(fn ->
        Ecto.Adapters.SQL.query(repository, "DROP TABLE log_events;")
      end)

      %{repository: repository}
    end

    test "creates a new table for log_events in target repository", %{repository: repository} do
      assert Repo.create_log_event_table(repository) == :ok
    end
  end
end
