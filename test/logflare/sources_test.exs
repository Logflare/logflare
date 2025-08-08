defmodule Logflare.SourcesTest do
  use Logflare.DataCase

  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Source.V1SourceSup
  alias Logflare.Backends
  alias Logflare.Users
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.V1SourceDynSup

  describe "create_source/2" do
    setup do
      user = insert(:user)
      %{user: user}
    end

    test "creates a source for a given user and creates schema", %{
      user: %{id: user_id} = user
    } do
      insert(:plan, name: "Free")
      assert {:ok, source} = Sources.create_source(%{name: TestUtils.random_string()}, user)
      assert %Source{user_id: ^user_id, v2_pipeline: false} = source
      assert SourceSchemas.get_source_schema_by(source_id: source.id)
    end

    test "creates a source with different retention", %{
      user: user
    } do
      insert(:plan, name: "Free", limit_source_ttl: :timer.hours(24) * 20)

      assert {:ok, %Source{retention_days: 10}} =
               Sources.create_source(%{name: "some name", retention_days: 10}, user)
    end
  end

  describe "update_source/2 with different retention_days" do
    setup do
      user = insert(:user)

      %{user: user}
    end

    test "valid retention days", %{
      user: user
    } do
      Logflare.Google.BigQuery
      |> expect(:patch_table_ttl, fn _source_id, _table_ttl, _dataset_id, _project_id ->
        {:ok, %Tesla.Env{}}
      end)

      insert(:plan, name: "Free", limit_source_ttl: :timer.hours(24) * 20)
      source = insert(:source, user: user)

      assert {:ok, %Source{retention_days: 12, bigquery_table_ttl: 12}} =
               Sources.update_source(source, %{retention_days: 12})
    end

    test "retention days exceeds", %{user: user} do
      insert(:plan, name: "Free", limit_source_ttl: :timer.hours(24))
      source = insert(:source, user: user)
      assert {:error, %Ecto.Changeset{}} = Sources.update_source(source, %{retention_days: 12})
    end
  end

  describe "update_source_by_user/2 disable/enable tailing" do
    setup do
      %{user: insert(:user)}
    end

    test "valid", %{user: user} do
      insert(:plan, name: "Free")
      source = insert(:source, user: user)

      assert {:ok, %Source{disable_tailing: true}} =
               Sources.update_source_by_user(source, %{disable_tailing: true})
    end
  end

  describe "list_sources_by_user/1" do
    setup do
      insert(:plan)
      :ok
    end

    test "lists sources for a given user" do
      user = insert(:user)
      insert(:source, user: user)
      assert [%Source{}] = Sources.list_sources_by_user(user)
      assert [] == insert(:user) |> Sources.list_sources_by_user()
    end
  end

  describe "get_bq_schema/1" do
    setup do
      user = Users.get_by(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
      source = insert(:source, token: TestUtils.gen_uuid(), rules: [], user_id: user.id)
      plan = insert(:plan, limit_source_fields_limit: 500)

      start_supervised!(Schema,
        source: source,
        plan: plan,
        bigquery_project_id: "some-id",
        bigquery_dataset_id: "some-id"
      )

      %{source: source}
    end

    @tag :failing
    test "fetches schema for given source", %{source: source, user: user} do
      source_id = source.token

      %{
        bigquery_table_ttl: bigquery_table_ttl,
        bigquery_dataset_location: bigquery_dataset_location,
        bigquery_project_id: bigquery_project_id,
        bigquery_dataset_id: bigquery_dataset_id
      } = GenUtils.get_bq_user_info(source_id)

      BigQuery.init_table!(
        user.id,
        source_id,
        bigquery_project_id,
        bigquery_table_ttl,
        bigquery_dataset_location,
        bigquery_dataset_id
      )

      schema = %GoogleApi.BigQuery.V2.Model.TableSchema{
        fields: [
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "event_message",
            policyTags: nil,
            type: "STRING"
          },
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "NULLABLE",
            name: "id",
            policyTags: nil,
            type: "STRING"
          },
          %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
            categories: nil,
            description: nil,
            fields: nil,
            mode: "REQUIRED",
            name: "timestamp",
            policyTags: nil,
            type: "TIMESTAMP"
          }
        ]
      }

      assert {:ok, _} =
               BigQuery.patch_table(source_id, schema, bigquery_dataset_id, bigquery_project_id)

      {:ok, left_schema} = Sources.get_bq_schema(source)
      assert left_schema == schema
    end
  end

  describe "preload_for_dashboard/1" do
    setup do
      insert(:plan)
      [user: insert(:user)]
    end

    test "preloads required fields", %{user: user} do
      sources = insert_list(3, :source, %{user: user})
      sources = Sources.preload_for_dashboard(sources)

      assert Enum.all?(sources, &Ecto.assoc_loaded?(&1.user))
      assert Enum.all?(sources, &Ecto.assoc_loaded?(&1.rules))
      assert Enum.all?(sources, &Ecto.assoc_loaded?(&1.saved_searches))
    end

    test "sorts data by name and favorite flag", %{user: user} do
      source_1 = insert(:source, %{user: user, name: "C"})
      source_2 = insert(:source, %{user: user, name: "B", favorite: true})
      source_3 = insert(:source, %{user: user, name: "A"})
      sources = Sources.preload_for_dashboard([source_1, source_2, source_3])

      assert Enum.map(sources, & &1.name) == Enum.map([source_2, source_3, source_1], & &1.name)
    end
  end

  for {prefix, flag, mod, dynsup} <- [
        {:v1, false, V1SourceSup, V1SourceDynSup},
        {:v2, true, Logflare.Backends.SourceSup, Logflare.Backends.SourcesSup}
      ] do
    describe "#{prefix} #{mod} - Source.Supervisor" do
      setup do
        Logflare.Google.BigQuery
        |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

        insert(:plan)

        on_exit(fn ->
          for {_id, child, _, _} <- DynamicSupervisor.which_children(unquote(dynsup)) do
            DynamicSupervisor.terminate_child(unquote(dynsup), child)
          end
        end)

        {:ok, user: insert(:user), mod: unquote(mod), flag: unquote(flag)}
      end

      test "start_source/1, lookup/2, delete_source/1", %{user: user, flag: flag, mod: mod} do
        Logflare.Google.BigQuery
        |> expect(:delete_table, fn _token -> :ok end)
        |> expect(:init_table!, fn _, _, _, _, _, _ -> :ok end)

        %{token: token} = insert(:source, user: user, v2_pipeline: flag)
        start_supervised!(Source.Supervisor)
        # TODO: cast should return :ok
        assert {:ok, ^token} = Source.Supervisor.start_source(token)
        :timer.sleep(500)
        assert {:ok, _pid} = Backends.lookup(mod, token)
        :timer.sleep(1_000)
        assert {:ok, ^token} = Source.Supervisor.delete_source(token)
        :timer.sleep(1000)
        assert {:error, :not_started} = Backends.lookup(mod, token)
      end

      test "reset_source/1", %{user: user, mod: mod, flag: flag} do
        %{token: token} = insert(:source, user: user, v2_pipeline: flag)
        start_supervised!(Source.Supervisor)
        # TODO: cast should return :ok
        assert {:ok, ^token} = Source.Supervisor.start_source(token)
        :timer.sleep(500)
        assert {:ok, pid} = Backends.lookup(mod, token)
        assert {:ok, ^token} = Source.Supervisor.reset_source(token)
        :timer.sleep(1500)
        assert {:ok, new_pid} = Backends.lookup(mod, token)
        assert new_pid != pid
      end

      test "able to start supervision tree", %{user: user, mod: mod, flag: flag} do
        source = insert(:source, user_id: user.id, v2_pipeline: flag)

        start_supervised!(Source.Supervisor)
        assert :ok = Source.Supervisor.ensure_started(source)
        :timer.sleep(1000)
        assert {:ok, _pid} = Backends.lookup(mod, source.token)
        assert Backends.cached_pending_buffer_len(source) == 0
      end

      test "able to reset supervision tree", %{user: user, mod: mod, flag: flag} do
        source = insert(:source, user_id: user.id, v2_pipeline: flag)

        start_supervised!(Source.Supervisor)
        assert :ok = Source.Supervisor.ensure_started(source)
        :timer.sleep(1000)
        assert {:ok, pid} = Backends.lookup(mod, source.token)
        assert {:ok, _} = Source.Supervisor.reset_source(source.token)
        assert {:ok, _} = Source.Supervisor.reset_source(source.token)
        :timer.sleep(3000)
        assert {:ok, new_pid} = Backends.lookup(RecentEventsTouch, source.token)
        assert pid != new_pid
        assert Backends.cached_pending_buffer_len(source) == 0
      end

      test "concurrent start attempts", %{user: user, mod: mod, flag: flag} do
        source = insert(:source, user_id: user.id, v2_pipeline: flag)
        start_supervised!(Source.Supervisor)
        assert :ok = Source.Supervisor.ensure_started(source)

        assert :ok = Source.Supervisor.ensure_started(source)
        assert :ok = Source.Supervisor.ensure_started(source)
        assert :ok = Source.Supervisor.ensure_started(source)
        assert :ok = Source.Supervisor.ensure_started(source)
        :timer.sleep(3000)
        assert {:ok, _pid} = Backends.lookup(mod, source.token)
        assert Backends.cached_pending_buffer_len(source) == 0
      end

      test "terminating Source.Supervisor does not bring everything down", %{
        user: user,
        mod: mod,
        flag: flag
      } do
        source = insert(:source, user_id: user.id, v2_pipeline: flag)
        pid = start_supervised!(Source.Supervisor)
        assert :ok = Source.Supervisor.ensure_started(source)
        :timer.sleep(3000)
        assert {:ok, prev_pid} = Backends.lookup(mod, source.token)
        Process.exit(pid, :kill)
        assert {:ok, pid} = Backends.lookup(mod, source.token)
        assert prev_pid == pid
      end

      test "should start broadcasting metrics on ingest", %{user: user, mod: mod, flag: flag} do
        source = insert(:source, user_id: user.id, v2_pipeline: flag)
        pid = start_supervised!(Source.Supervisor)
        assert :ok = Source.Supervisor.ensure_started(source)
        :timer.sleep(3000)
        assert {:ok, prev_pid} = Backends.lookup(mod, source.token)
        Process.exit(pid, :kill)
        assert {:ok, pid} = Backends.lookup(mod, source.token)
        assert prev_pid == pid
      end
    end
  end

  describe "Source.Supervisor v1-v2 interop" do
    setup do
      Logflare.Google.BigQuery
      |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      insert(:plan)
      start_supervised!(Source.Supervisor)

      on_exit(fn ->
        for dynsup <- [V1SourceDynSup, Backends.SourcesSup],
            {_id, child, _, _} <- DynamicSupervisor.which_children(dynsup) do
          DynamicSupervisor.terminate_child(dynsup, child)
        end
      end)

      {:ok, user: insert(:user)}
    end

    test "if initially v1 sup running, should seamlesssly transition to v2 source sup", %{
      user: user
    } do
      source = insert(:source, user: user)
      assert :ok = Source.Supervisor.ensure_started(source)
      :timer.sleep(500)
      assert {:ok, v1_pid} = Backends.lookup(V1SourceSup, source.token)
      # v2 not started yet
      assert {:error, _} = Backends.lookup(Backends.SourceSup, source.token)

      # update to v2
      assert {:ok, source} = Sources.update_source_by_user(source, %{v2_pipeline: true})
      assert {:ok, _} = Source.Supervisor.reset_source(source.token)

      :timer.sleep(600)
      assert {:ok, v2_pid} = Backends.lookup(Backends.SourceSup, source.token)
      # v1 not started
      assert {:error, _} = Backends.lookup(V1SourceSup, source.token)
      assert v1_pid != v2_pid
    end

    test "if initially v2 sup running, should seamlessly transition to v1 source sup", %{
      user: user
    } do
      source = insert(:source, user: user, v2_pipeline: true)
      assert :ok = Source.Supervisor.ensure_started(source)
      :timer.sleep(500)
      assert {:ok, v2_pid} = Backends.lookup(Backends.SourceSup, source.token)
      # v1 not started yet
      assert {:error, _} = Backends.lookup(V1SourceSup, source.token)

      :timer.sleep(100)

      # update to v1
      assert {:ok, source} = Sources.update_source_by_user(source, %{v2_pipeline: false})
      assert {:ok, _} = Source.Supervisor.reset_source(source.token)

      :timer.sleep(600)
      assert {:ok, v1_pid} = Backends.lookup(V1SourceSup, source.token)
      # v2 should be killed
      assert {:error, _} = Backends.lookup(Backends.SourceSup, source.token)
      assert v1_pid != v2_pid
    end
  end

  test "ingest_ets_tables_started?/0" do
    assert Sources.ingest_ets_tables_started?()
  end

  describe "list_sources/1" do
    test "list_sources/1 with backend_id filter" do
      user = insert(:user)
      backend1 = insert(:backend, user: user)
      backend2 = insert(:backend, user: user)

      source1 = insert(:source, user: user, v2_pipeline: true, v2_backend_id: backend1.id)
      source2 = insert(:source, user: user, v2_pipeline: true, v2_backend_id: backend2.id)
      source3 = insert(:source, user: user, v2_pipeline: true, v2_backend_id: backend1.id)

      results = Sources.list_sources(backend_id: backend1.id)
      assert length(results) == 2

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [source1.id, source3.id] |> Enum.sort()
      assert result_ids == expected_ids
    end

    test "list_sources/1 with default_ingest_backend_enabled? filter" do
      user = insert(:user)

      source1 = insert(:source, user: user, default_ingest_backend_enabled?: true)
      _source2 = insert(:source, user: user, default_ingest_backend_enabled?: false)
      source3 = insert(:source, user: user, default_ingest_backend_enabled?: true)

      results = Sources.list_sources(default_ingest_backend_enabled?: true)
      assert length(results) == 2

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [source1.id, source3.id] |> Enum.sort()
      assert result_ids == expected_ids

      assert Enum.all?(results, &(&1.default_ingest_backend_enabled? == true))
    end

    test "list_sources/1 with combined backend_id and default_ingest filters" do
      user = insert(:user)
      backend = insert(:backend, user: user)

      source1 =
        insert(:source,
          user: user,
          v2_pipeline: true,
          v2_backend_id: backend.id,
          default_ingest_backend_enabled?: true
        )

      _source2 =
        insert(:source,
          user: user,
          v2_pipeline: true,
          v2_backend_id: backend.id,
          default_ingest_backend_enabled?: false
        )

      source3 =
        insert(:source,
          user: user,
          v2_pipeline: true,
          v2_backend_id: backend.id,
          default_ingest_backend_enabled?: true
        )

      results =
        Sources.list_sources(
          backend_id: backend.id,
          default_ingest_backend_enabled?: true
        )

      assert length(results) == 2
      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [source1.id, source3.id] |> Enum.sort()
      assert result_ids == expected_ids
    end

    test "list_sources/1 with v2_pipeline filter" do
      user = insert(:user)

      source1 = insert(:source, user: user, v2_pipeline: true)
      source2 = insert(:source, user: user, v2_pipeline: false)
      source3 = insert(:source, user: user, v2_pipeline: true)

      v2_results = Sources.list_sources(v2_pipeline: true)
      assert length(v2_results) == 2

      v2_ids = Enum.map(v2_results, & &1.id) |> Enum.sort()
      expected_v2_ids = [source1.id, source3.id] |> Enum.sort()
      assert v2_ids == expected_v2_ids

      v1_results = Sources.list_sources(v2_pipeline: false)
      assert length(v1_results) == 1
      assert hd(v1_results).id == source2.id
    end

    test "list_sources/1 ignores unknown filters" do
      user = insert(:user)
      source1 = insert(:source, user: user)
      source2 = insert(:source, user: user)

      # Unknown filter should be ignored
      results = Sources.list_sources(unknown_filter: "ignored", user_id: user.id)
      assert length(results) == 2

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [source1.id, source2.id] |> Enum.sort()
      assert result_ids == expected_ids
    end
  end
end
