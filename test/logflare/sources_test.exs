defmodule Logflare.SourcesTest do
  use Logflare.DataCase

  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Sources.Source
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Backends
  alias Logflare.Users
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Backends.SourceRegistry
  alias Logflare.Backends.SourceSup

  describe "changesets" do
    setup do
      [plan: insert(:plan)]
    end

    test "update_by_user_changeset" do
      source = insert(:source, user: insert(:user), labels: "initial=label")

      assert %Ecto.Changeset{changes: changes} = Source.update_by_user_changeset(source, %{})
      assert changes == %{}

      assert %Ecto.Changeset{changes: changes} =
               Source.update_by_user_changeset(source, %{labels: "test=some_label"})

      assert changes == %{labels: "test=some_label"}
    end

    test "update_by_user_changeset trims description and turns blank into nil" do
      source = insert(:source, user: insert(:user), description: "existing description")

      assert %Ecto.Changeset{changes: %{description: "trimmed description"}} =
               Source.update_by_user_changeset(source, %{description: "  trimmed description  "})

      assert %Ecto.Changeset{changes: %{description: nil}} =
               Source.update_by_user_changeset(source, %{description: "   "})
    end
  end

  describe "recommended_query_fields/1" do
    test "preserves required marker in suggested keys" do
      source =
        build(:source,
          bigquery_clustering_fields: "session_id",
          suggested_keys: "metadata.level!,m.user_id"
        )

      assert Source.recommended_query_fields(source) == [
               "session_id",
               "metadata.level!",
               "m.user_id"
             ]
    end

    test "query_field_name/1 and required_query_field?/1 normalize correctly" do
      assert Source.query_field_name("metadata.level!") == "metadata.level"
      assert Source.query_field_name("metadata.level") == "metadata.level"
      assert Source.required_query_field?("metadata.level!")
      refute Source.required_query_field?("metadata.level")
    end
  end

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
      assert %Source{user_id: ^user_id} = source
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

  describe "get_source_by_user_access_and_preload/2" do
    test "get_source_by_user_access_and_preload/2" do
      insert(:plan)
      owner = insert(:user)
      team_user = insert(:team_user, email: owner.email)
      %Source{id: source_id} = insert(:source, user: owner)
      %Source{id: other_source_id} = insert(:source, user: team_user.team.user)
      %Source{id: forbidden_source_id} = insert(:source, user: build(:user))

      assert %Source{id: ^source_id} =
               Sources.get_by_user_access(owner, source_id)

      assert %Source{id: ^source_id} =
               Sources.get_by_user_access(team_user, source_id)

      assert %Source{id: ^other_source_id} =
               Sources.get_by_user_access(team_user, other_source_id)

      assert nil == Sources.get_by_user_access(owner, forbidden_source_id)
      assert nil == Sources.get_by_user_access(team_user, forbidden_source_id)
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

    test "preloads only the required fields", %{user: user} do
      sources = insert_list(3, :source, %{user_id: user.id})
      assocs = Sources.Source.__schema__(:associations)
      sources = Enum.map(sources, &Ecto.reset_fields(&1, assocs))
      sources = Sources.preload_for_dashboard(sources)

      assert Enum.all?(sources, &Ecto.assoc_loaded?(&1.user))

      refute Enum.any?(sources, &Ecto.assoc_loaded?(&1.rules))
    end

    test "sorts data by name and favorite flag", %{user: user} do
      source_1 = insert(:source, %{user: user, name: "C"})
      source_2 = insert(:source, %{user: user, name: "B", favorite: true})
      source_3 = insert(:source, %{user: user, name: "A"})
      sources = Sources.preload_for_dashboard([source_1, source_2, source_3])

      assert Enum.map(sources, & &1.name) == Enum.map([source_2, source_3, source_1], & &1.name)
    end
  end

  describe "Source.Supervisor" do
    setup do
      Logflare.Google.BigQuery
      |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      insert(:plan)

      on_exit(fn ->
        for {_id, child, _, _} <- DynamicSupervisor.which_children(Logflare.Backends.SourcesSup) do
          DynamicSupervisor.terminate_child(Logflare.Backends.SourcesSup, child)
        end
      end)

      {:ok, user: insert(:user)}
    end

    test "start_source/1, lookup/2, delete_source/1", %{user: user} do
      Logflare.Google.BigQuery
      |> expect(:delete_table, fn _token -> :ok end)
      |> expect(:init_table!, fn _, _, _, _, _, _ -> :ok end)

      %{token: token} = insert(:source, user: user)
      start_supervised!(Source.Supervisor)
      # TODO: cast should return :ok
      assert {:ok, ^token} = Source.Supervisor.start_source(token)
      :timer.sleep(500)
      assert {:ok, _pid} = Backends.lookup(SourceSup, token)
      :timer.sleep(1_000)
      assert {:ok, ^token} = Source.Supervisor.delete_source(token)
      :timer.sleep(1000)
      assert {:error, :not_started} = Backends.lookup(SourceSup, token)
    end

    test "reset_source/1", %{user: user} do
      %{token: token} = insert(:source, user: user)
      start_supervised!(Source.Supervisor)
      # TODO: cast should return :ok
      assert {:ok, ^token} = Source.Supervisor.start_source(token)
      :timer.sleep(500)
      assert {:ok, pid} = Backends.lookup(SourceSup, token)
      assert {:ok, ^token} = Source.Supervisor.reset_source(token)
      :timer.sleep(1500)
      assert {:ok, new_pid} = Backends.lookup(SourceSup, token)
      assert new_pid != pid
    end

    test "able to start supervision tree", %{user: user} do
      source = insert(:source, user_id: user.id)

      start_supervised!(Source.Supervisor)
      assert :ok = Source.Supervisor.ensure_started(source)
      :timer.sleep(1000)
      assert {:ok, _pid} = Backends.lookup(SourceSup, source.token)
      assert Backends.cached_pending_buffer_len(source) == 0
    end

    test "able to reset supervision tree", %{user: user} do
      source = insert(:source, user_id: user.id)

      start_supervised!(Source.Supervisor)
      assert :ok = Source.Supervisor.ensure_started(source)
      :timer.sleep(3000)
      assert {:ok, pid} = Backends.lookup(SourceSup, source.token)
      assert {:ok, _} = Source.Supervisor.reset_source(source.token)
      assert {:ok, _} = Source.Supervisor.reset_source(source.token)
      :timer.sleep(3000)
      assert {:ok, new_pid} = Backends.lookup(SourceSup, source.token)
      assert pid != new_pid
      assert Backends.cached_pending_buffer_len(source) == 0
    end

    test "concurrent start attempts", %{user: user} do
      source = insert(:source, user_id: user.id)
      start_supervised!(Source.Supervisor)
      assert :ok = Source.Supervisor.ensure_started(source)

      assert :ok = Source.Supervisor.ensure_started(source)
      assert :ok = Source.Supervisor.ensure_started(source)
      assert :ok = Source.Supervisor.ensure_started(source)
      assert :ok = Source.Supervisor.ensure_started(source)
      :timer.sleep(3000)
      assert {:ok, _pid} = Backends.lookup(SourceSup, source.token)
      assert Backends.cached_pending_buffer_len(source) == 0
    end

    test "terminating Source.Supervisor does not bring everything down", %{user: user} do
      source = insert(:source, user_id: user.id)
      pid = start_supervised!(Source.Supervisor)
      assert :ok = Source.Supervisor.ensure_started(source)
      :timer.sleep(3000)
      assert {:ok, prev_pid} = Backends.lookup(SourceSup, source.token)
      Process.exit(pid, :kill)
      assert {:ok, pid} = Backends.lookup(SourceSup, source.token)
      assert prev_pid == pid
    end

    test "should start broadcasting metrics on ingest", %{user: user} do
      source = insert(:source, user_id: user.id)
      pid = start_supervised!(Source.Supervisor)
      assert :ok = Source.Supervisor.ensure_started(source)
      :timer.sleep(3000)
      assert {:ok, prev_pid} = Backends.lookup(SourceSup, source.token)
      Process.exit(pid, :kill)
      assert {:ok, pid} = Backends.lookup(SourceSup, source.token)
      assert prev_pid == pid
    end
  end

  test "ingest_ets_tables_started?/0" do
    assert Sources.ingest_ets_tables_started?()
  end

  describe "shutdown_idle_sources/0" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)
      Sources.Cache.get_by_id(source.id)
      :ok = Backends.start_source_sup(source)
      {:ok, source: source, user: user}
    end

    test "shuts down sources with 0 avg rate and 0 pending items", %{source: source} do
      # Add a 10-minute old event
      ten_minutes_ago = DateTime.utc_now() |> DateTime.add(-10 * 60, :second)
      old_event = build(:log_event, source: source, ingested_at: ten_minutes_ago)
      Backends.IngestEventQueue.add_to_table({source.id, nil, nil}, [old_event])
      Backends.IngestEventQueue.mark_ingested({source.id, nil, nil}, [old_event])

      TestUtils.retry_assert(fn ->
        assert Backends.source_sup_started?(source)
        assert Sources.get_source_metrics_for_ingest(source).avg == 0
        assert [_event] = Backends.list_recent_logs_local(source, 1)
      end)

      :ok = Sources.shutdown_idle_sources()

      TestUtils.retry_assert(fn ->
        refute Backends.source_sup_started?(source)
      end)
    end

    test "does NOT shut down sources with active ingest", %{source: source} do
      Logflare.Sources.Counters.increment(source.token, 1)
      Logflare.Sources.Source.RateCounterServer.handle_info(:put_rate, source.token)

      assert Sources.get_source_metrics_for_ingest(source).avg > 0

      :ok = Sources.shutdown_idle_sources()
      assert Backends.source_sup_started?(source)
    end

    test "does NOT shut down sources with pending items", %{source: source} do
      event = build(:log_event, source: source)
      Backends.IngestEventQueue.add_to_table({source.id, nil}, [event])
      assert Backends.source_sup_started?(source)
      :ok = Sources.shutdown_idle_sources()
      assert Backends.source_sup_started?(source)
    end

    test "does NOT shut down sources with recent logs within 5 minutes", %{source: source} do
      event = build(:log_event, source: source, ingested_at: DateTime.utc_now())
      Backends.IngestEventQueue.add_to_table({source.id, nil, nil}, [event])
      Backends.IngestEventQueue.mark_ingested({source.id, nil, nil}, [event])

      TestUtils.retry_assert(fn ->
        assert [_event] = Backends.list_recent_logs_local(source, 1)
      end)

      :ok = Sources.shutdown_idle_sources()
      assert Backends.source_sup_started?(source)
    end
  end

  describe "list_sources/1" do
    setup do
      insert(:plan)
      [user: insert(:user)]
    end

    test "list_sources/1 with backend_id filter", %{user: user} do
      backend1 = insert(:backend, user: user)
      backend2 = insert(:backend, user: user)

      source1 = insert(:source, user: user)
      source2 = insert(:source, user: user)
      source3 = insert(:source, user: user)

      {:ok, _} = Logflare.Backends.update_source_backends(source1, [backend1])
      {:ok, _} = Logflare.Backends.update_source_backends(source2, [backend2])
      {:ok, _} = Logflare.Backends.update_source_backends(source3, [backend1])

      results = Sources.list_sources(backend_id: backend1.id)
      assert length(results) == 2

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [source1.id, source3.id] |> Enum.sort()
      assert result_ids == expected_ids
    end

    test "list_sources/1 with default_ingest_backend_enabled? filter", %{user: user} do
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

    test "list_sources/1 with combined backend_id and default_ingest filters", %{user: user} do
      backend = insert(:backend, user: user)

      source1 =
        insert(:source,
          user: user,
          default_ingest_backend_enabled?: true
        )

      source2 =
        insert(:source,
          user: user,
          default_ingest_backend_enabled?: false
        )

      source3 =
        insert(:source,
          user: user,
          default_ingest_backend_enabled?: true
        )

      {:ok, _} = Logflare.Backends.update_source_backends(source1, [backend])
      {:ok, _} = Logflare.Backends.update_source_backends(source2, [backend])
      {:ok, _} = Logflare.Backends.update_source_backends(source3, [backend])

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

    test "list_sources/1 ignores unknown filters", %{user: user} do
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

  describe "stop_source_local/1" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source}
    end

    test "stops SourceSup and doesn't restart", %{source: source} do
      assert :ok = Backends.start_source_sup(source)
      assert {:ok, pid} = Backends.lookup(SourceSup, source)
      assert [{^pid, _}] = Registry.lookup(SourceRegistry, {source.id, SourceSup})

      assert :ok = Source.Supervisor.stop_source_local(source)
      Process.sleep(100)

      refute Process.alive?(pid)
      assert [] = Registry.lookup(SourceRegistry, {source.id, SourceSup})
      assert {:error, :not_started} = Backends.lookup(SourceSup, source)
    end

    test "abnormal exit restarts SourceSup", %{source: source} do
      assert :ok = Backends.start_source_sup(source)
      assert {:ok, pid} = Backends.lookup(SourceSup, source)

      Logflare.Utils.try_to_stop_process(pid, :abnormal)

      refute Process.alive?(pid)

      TestUtils.retry_assert(fn ->
        assert {:ok, pid2} = Backends.lookup(SourceSup, source)
        assert pid != pid2
      end)
    end
  end

  describe "create_user_system_sources/1" do
    setup do
      insert(:plan)
      user = insert(:user)
      %{user: user}
    end

    test "creates system sources with correct partition type", %{user: user} do
      sources = Sources.create_user_system_sources(user.id)

      assert length(sources) == length(Source.system_source_types())

      for source <- sources do
        assert source.system_source == true
        assert source.system_source_type in Source.system_source_types()
        assert source.favorite == true
        assert source.bq_table_partition_type == :timestamp
        assert source.user_id == user.id
        assert String.starts_with?(source.name, "system.")
      end
    end
  end

  describe "labels validation and normalization" do
    setup do
      insert(:plan)
      user = insert(:user)
      %{source: insert(:source, user_id: user.id)}
    end

    test "normalizes whitespace around commas and equals", %{source: source} do
      for {input, expected} <- [
            {"key1=value1 , key2=value2", "key1=value1,key2=value2"},
            {"key1 = value1,key2= value2", "key1=value1,key2=value2"},
            {"  key1=value1 , key2=value2  ", "key1=value1,key2=value2"},
            {"key1=value1,,key2=value2,", "key1=value1,key2=value2"}
          ] do
        assert %_{valid?: true} = changeset = Source.changeset(source, %{labels: input})
        assert get_change(changeset, :labels) == expected
      end
    end

    test "accepts valid formats and empty string", %{source: source} do
      assert Source.changeset(source, %{labels: ""}).valid?
      assert Source.changeset(source, %{labels: "status=m.level,project=name"}).valid?
    end

    test "rejects invalid label formats", %{source: source} do
      invalid_cases = [
        {"no_equals", "each label must be in key=value format"},
        {"=value", "each label must have a non-empty key"},
        {"key=", "each label must have a non-empty value"},
        {"key=val=extra", "each label must have exactly one '=' sign"},
        {"valid=ok,invalid", "each label must be in key=value format"}
      ]

      for {input, expected_error} <- invalid_cases do
        assert %_{valid?: false} = changeset = Source.changeset(source, %{labels: input})
        assert expected_error in errors_on(changeset).labels
      end
    end
  end

  describe "recent events touch" do
    setup do
      insert(:plan)
      user = insert(:user)
      {:ok, user: user}
    end

    test "updates source.log_events_updated_at", %{
      user: user
    } do
      timestamp =
        NaiveDateTime.utc_now() |> NaiveDateTime.add(-2, :hour) |> NaiveDateTime.truncate(:second)

      source = insert(:source, user_id: user.id, log_events_updated_at: timestamp)
      Sources.Cache.get_by_id(source.id)
      start_supervised!({SourceSup, source})
      :timer.sleep(800)
      timestamp = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
      assert {:ok, 1} = Sources.recent_events_touch(timestamp)
      updated = Sources.get_by(id: source.id)
      assert updated.log_events_updated_at == timestamp

      # does not update again if recently updated
      Sources.Cache.get_by_id(source.id)
      :timer.sleep(1_000)
      timestamp = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
      assert {:ok, 0} = Sources.recent_events_touch(timestamp)

      updated = Sources.get_by(id: source.id)
      assert updated.log_events_updated_at != timestamp
    end
  end

  describe "get_by_name_and_user_access/2" do
    test "returns source when user owns it" do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user, name: "my_source")

      result = Sources.get_by_name_and_user_access(user, "my_source")

      assert %Logflare.Sources.Source{id: id} = result
      assert id == source.id
    end

    test "returns source when team_user has access" do
      insert(:plan)
      team_owner = insert(:user)
      team = insert(:team, user: team_owner)
      source = insert(:source, user: team_owner, name: "team_source")

      team_user = insert(:team_user, email: "member@example.com", team: team)

      result = Sources.get_by_name_and_user_access(team_user, "team_source")

      assert %Logflare.Sources.Source{id: id} = result
      assert id == source.id
    end

    test "returns nil when user does not have access" do
      insert(:plan)
      user1 = insert(:user)
      user2 = insert(:user)
      insert(:source, user: user1, name: "private_source")

      result = Sources.get_by_name_and_user_access(user2, "private_source")

      assert is_nil(result),
             "Expected nil when user2 does not have access to user1's source, got: #{inspect(result)}"
    end

    test "returns nil when source does not exist" do
      insert(:plan)
      user = insert(:user)

      result = Sources.get_by_name_and_user_access(user, "nonexistent_source_name")

      assert is_nil(result),
             "Expected nil when querying for non-existent source name, got: #{inspect(result)}"
    end
  end
end
