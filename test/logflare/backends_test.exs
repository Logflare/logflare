defmodule Logflare.BackendsTest do
  use Logflare.DataCase

  import StreamData
  import ExUnitProperties

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.SourceSupWorker
  alias Logflare.Logs.SourceRouting
  alias Logflare.Lql
  alias Logflare.PubSubRates
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.BigQuery.Pipeline
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.User

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "encryption" do
    test "backend config is encrypted to the :config_encrypted field" do
      insert(:backend, config_encrypted: %{some_value: "testing"})

      assert [
               %{
                 config: nil,
                 config_encrypted: encrypted
               }
             ] = Repo.all(from b in "backends", select: [:config, :config_encrypted])

      assert is_binary(encrypted)
    end
  end

  describe "backend management" do
    setup do
      user = insert(:user)
      [source: insert(:source, user_id: user.id), user: user]
    end

    test "list_backends/1 with metadata" do
      backend = insert(:backend, metadata: %{some: "data", value: true, other: false})

      assert [result] =
               Backends.list_backends(metadata: %{some: "data", value: "true", other: false})

      assert result.id == backend.id
    end

    test "list_backends/1 with default_ingest filter", %{user: user} do
      backend1 =
        insert(:backend,
          user: user,
          type: :bigquery,
          config: %{project_id: "test", dataset_id: "test"},
          default_ingest?: true
        )

      _backend2 =
        insert(:backend,
          user: user,
          type: :webhook,
          config: %{url: "http://test.com"},
          default_ingest?: false
        )

      backend3 =
        insert(:backend,
          user: user,
          type: :postgres,
          config: %{url: "postgres://test"},
          default_ingest?: true
        )

      results = Backends.list_backends(default_ingest?: true, user_id: user.id)
      assert length(results) == 2

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [backend1.id, backend3.id] |> Enum.sort()
      assert result_ids == expected_ids

      assert Enum.all?(results, &(&1.default_ingest? == true))
    end

    test "list_backends/1 with default_ingest and source_id filters", %{
      source: source,
      user: user
    } do
      backend1 =
        insert(:backend,
          user: user,
          type: :bigquery,
          config: %{project_id: "test", dataset_id: "test"},
          default_ingest?: true
        )

      backend2 =
        insert(:backend,
          user: user,
          type: :clickhouse,
          config: %{url: "http://ch", database: "test", port: 8123},
          default_ingest?: true
        )

      backend3 =
        insert(:backend,
          user: user,
          type: :webhook,
          config: %{url: "http://test.com"},
          default_ingest?: false
        )

      backend4 =
        insert(:backend,
          user: user,
          type: :postgres,
          config: %{url: "postgres://test"},
          default_ingest?: true
        )

      assert {:ok, _} = Backends.update_source_backends(source, [backend1, backend2, backend3])

      results = Backends.list_backends(source_id: source.id, default_ingest?: true)
      assert length(results) == 2

      result_ids = Enum.map(results, & &1.id) |> Enum.sort()
      expected_ids = [backend1.id, backend2.id] |> Enum.sort()
      assert result_ids == expected_ids

      refute backend4.id in result_ids
    end

    test "list_backends/1 with alerts queries", %{user: user} do
      backend = insert(:backend, user: user)
      insert(:alert, user: user, backends: [backend])

      assert [%{alert_queries: [_]}] =
               Backends.list_backends(user_id: user.id) |> Backends.preload_alerts()
    end

    test "fetch_latest_timestamp/1 without SourceSup returns 0", %{source: source} do
      assert 0 == Backends.fetch_latest_timestamp(source)
    end

    test "create backend", %{user: user} do
      assert {:ok, %Backend{}} =
               Backends.create_backend(%{
                 name: "some name",
                 type: :webhook,
                 user_id: user.id,
                 config: %{url: "http://some.url"}
               })

      assert {:error, %Ecto.Changeset{}} =
               Backends.create_backend(%{name: "123", type: :other, config: %{}})

      assert {:error, %Ecto.Changeset{}} =
               Backends.create_backend(%{name: "123", type: :webhook, config: nil})

      # config validations
      assert {:error, %Ecto.Changeset{}} =
               Backends.create_backend(%{type: :postgres, config: %{url: nil}})
    end

    test "delete backend" do
      backend = insert(:backend)
      assert {:ok, %Backend{}} = Backends.delete_backend(backend)
      assert Backends.get_backend(backend.id) == nil
    end

    test "delete backend with rules" do
      user = insert(:user)
      source = insert(:source, user: user)
      insert(:rule, source: source)
      backend = insert(:backend, user: user)
      assert {:ok, %Backend{}} = Backends.delete_backend(backend)
      assert Backends.get_backend(backend.id) == nil
    end

    test "can attach multiple backends to a source", %{source: source} do
      [backend1, backend2] = insert_pair(:backend)
      assert [] = Backends.list_backends(source_id: source.id)
      assert {:ok, %Source{}} = Backends.update_source_backends(source, [backend1, backend2])
      assert [_, _] = Backends.list_backends(source_id: source.id)

      # removal
      assert {:ok, %Source{}} = Backends.update_source_backends(source, [])
      assert [] = Backends.list_backends(source_id: source.id)
    end

    test "can attach/remove multiple alerts to a backend", %{user: user} do
      [alert1, _] = alerts = insert_pair(:alert, user: user)
      backend = insert(:backend, user: user)
      # addition
      assert {:ok, %Backend{} = backend} =
               Backends.update_backend(backend, %{alert_queries: alerts})

      assert [%{alert_queries: [_, _]}] =
               Backends.list_backends(user_id: user.id) |> Backends.preload_alerts()

      # removal
      assert {:ok, %Backend{} = backend} =
               Backends.update_backend(backend, %{alert_queries: [alert1]})

      assert %Backend{alert_queries: [_]} = Backends.preload_alerts(backend)
    end

    test "update backend config correctly", %{user: user} do
      assert {:ok, backend} =
               Backends.create_backend(%{
                 name: "some name",
                 type: :webhook,
                 config: %{url: "http://example.com"},
                 user_id: user.id
               })

      assert {:error, %Ecto.Changeset{}} =
               Backends.create_backend(%{
                 type: :webhook,
                 config: nil
               })

      assert {:ok,
              %Backend{
                config: %{
                  url: "http://changed.com"
                }
              }} = Backends.update_backend(backend, %{config: %{url: "http://changed.com"}})

      assert {:error, %Ecto.Changeset{}} =
               Backends.update_backend(backend, %{config: %{url: nil}})

      # unchanged
      assert %Backend{config: %{url: "http" <> _}} = Backends.get_backend(backend.id)

      :timer.sleep(1000)
    end
  end

  describe "update_backend/2 with default_ingest?" do
    setup do
      insert(:plan)
      user = insert(:user)
      clickhouse_config = %{url: "http://localhost", database: "test", port: 8123}

      backend =
        insert(:backend,
          user: user,
          type: :clickhouse,
          default_ingest?: false,
          config: clickhouse_config
        )

      source = insert(:source, user: user)

      {:ok, user: user, backend: backend, source: source, config: clickhouse_config}
    end

    test "requires source_id when enabling default_ingest?", %{backend: backend} do
      assert {:error, changeset} =
               Backends.update_backend(backend, %{default_ingest?: true})

      assert "Please select a source when enabling default ingest" in errors_on(changeset).default_ingest?
    end

    test "creates source association when enabling default_ingest?", %{
      backend: backend,
      source: source
    } do
      {:ok, source} = Sources.update_source(source, %{default_ingest_backend_enabled?: true})

      assert {:ok, updated} =
               Backends.update_backend(backend, %{
                 default_ingest?: true,
                 source_id: to_string(source.id)
               })

      assert updated.default_ingest? == true

      source = Sources.get(source.id) |> Sources.preload_backends()
      assert Enum.any?(source.backends, &(&1.id == backend.id))
    end

    test "removes source associations when disabling default_ingest?", %{
      user: user,
      source: source,
      config: config
    } do
      {:ok, source} = Sources.update_source(source, %{default_ingest_backend_enabled?: true})

      backend =
        insert(:backend, user: user, type: :clickhouse, default_ingest?: true, config: config)

      assert {:ok, _} = Backends.update_source_backends(source, [backend])

      assert {:ok, updated} =
               Backends.update_backend(backend, %{default_ingest?: false})

      assert updated.default_ingest? == false

      source = Sources.get(source.id) |> Sources.preload_backends()
      assert Enum.empty?(source.backends)
    end

    test "does not duplicate source associations", %{user: user, source: source, config: config} do
      {:ok, source} = Sources.update_source(source, %{default_ingest_backend_enabled?: true})

      backend =
        insert(:backend, user: user, type: :clickhouse, default_ingest?: true, config: config)

      assert {:ok, _} = Backends.update_source_backends(source, [backend])

      assert {:ok, _updated} =
               Backends.update_backend(backend, %{
                 default_ingest?: true,
                 source_id: to_string(source.id)
               })

      source = Sources.get(source.id) |> Sources.preload_backends()
      backend_ids = Enum.map(source.backends, & &1.id)
      assert length(backend_ids) == 1
      assert backend.id in backend_ids
    end

    test "requires source to have default_ingest_backend_enabled?", %{
      backend: backend,
      source: source
    } do
      assert {:error, changeset} =
               Backends.update_backend(backend, %{
                 default_ingest?: true,
                 source_id: to_string(source.id)
               })

      assert "Source must have default ingest backend support enabled" in errors_on(changeset).default_ingest?
    end
  end

  describe "SourceSup management" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source, user: user}
    end

    test "ensure_source_sup_started/1", %{source: source} do
      assert :ok = Backends.ensure_source_sup_started(source)
    end

    test "on attach to source, update SourceSup", %{source: source} do
      [backend1, backend2] = insert_pair(:backend)
      start_supervised!({SourceSup, source})
      via = Backends.via_source(source, SourceSup)
      prev_length = Supervisor.which_children(via) |> length()
      assert {:ok, _} = Backends.update_source_backends(source, [backend1, backend2])

      new_length = Supervisor.which_children(via) |> length()
      assert new_length > prev_length

      # removal
      assert {:ok, _} = Backends.update_source_backends(source, [])
      assert Supervisor.which_children(via) |> length() < new_length
    end

    test "SourceSup source-backends get resolved by SourceSupWorker", %{source: source} do
      rule_backend = insert(:backend)
      start_supervised!({SourceSup, source})
      via = Backends.via_source(source, SourceSup)
      prev_length = Supervisor.which_children(via) |> length()
      # two source-backends from attached
      insert_pair(:backend, sources: [source])
      # one source-backend from rules
      insert_pair(:rule, source: source, backend: rule_backend)

      # start an out-of-tree SourceSupWorker
      start_supervised({SourceSupWorker, [source: source, interval: 100]})
      :timer.sleep(200)
      new_length = Supervisor.which_children(via) |> length()
      assert new_length > prev_length
      assert new_length - prev_length == 3

      Logflare.Repo.delete_all(Logflare.Rules.Rule)
      Logflare.Repo.delete_all(Logflare.Backends.SourcesBackend)
      Logflare.Repo.delete_all(Logflare.Backends.Backend)

      :timer.sleep(200)
      # removal
      new_length = Supervisor.which_children(via) |> length()
      assert new_length == prev_length
    end

    test "source_sup_started?/1, lookup/2", %{source: source} do
      assert false == Backends.source_sup_started?(source)
      start_supervised!({SourceSup, source})
      :timer.sleep(1000)
      assert true == Backends.source_sup_started?(source)
      assert {:ok, _pid} = Backends.lookup(RecentEventsTouch, source.token)
    end

    test "start_source_sup/1, stop_source_sup/1, restart_source_sup/1", %{source: source} do
      assert :ok = Backends.start_source_sup(source)
      assert {:error, :already_started} = Backends.start_source_sup(source)

      assert :ok = Backends.stop_source_sup(source)
      assert {:error, :not_started} = Backends.stop_source_sup(source)

      assert {:error, :not_started} = Backends.restart_source_sup(source)
      assert :ok = Backends.start_source_sup(source)
      assert :ok = Backends.restart_source_sup(source)
    end

    test "rules_child_started? when SourceSup already started", %{source: source} do
      rule_backend = insert(:backend)
      rule = insert(:rule, source: source, backend: rule_backend)
      refute SourceSup.rule_child_started?(rule)
      start_supervised!({SourceSup, source})

      TestUtils.retry_assert(fn ->
        assert SourceSup.rule_child_started?(rule)
      end)
    end
  end

  describe "RecentEventsTocuh" do
    setup do
      insert(:plan)
      user = insert(:user)
      timestamp = NaiveDateTime.utc_now()
      source = insert(:source, user_id: user.id, log_events_updated_at: timestamp)
      {:ok, _tid} = IngestEventQueue.upsert_tid({source.id, nil, nil})
      {:ok, source: source, timestamp: timestamp}
    end

    test "RecentEventsTouch updates source.log_events_updated_at", %{
      source: source
    } do
      le = build(:log_event, ingested_at: NaiveDateTime.utc_now() |> NaiveDateTime.add(200))
      IngestEventQueue.add_to_table({source.id, nil, nil}, [le])
      start_supervised!({RecentEventsTouch, source: source, touch_every: 100})
      :timer.sleep(800)
      updated = Sources.get_by(id: source.id)
      assert updated.log_events_updated_at != source.log_events_updated_at
      assert updated.log_events_updated_at == le.ingested_at |> NaiveDateTime.truncate(:second)
    end

    test "RecentEventsTouch does not update source.log_events_updated_at if already updated", %{
      source: source,
      timestamp: timestamp
    } do
      le = build(:log_event, ingested_at: timestamp)
      IngestEventQueue.add_to_table({source.id, nil, nil}, [le])
      start_supervised!({RecentEventsTouch, source: source, touch_every: 100})
      :timer.sleep(800)
      updated = Sources.get_by(id: source.id)
      assert updated.log_events_updated_at == source.log_events_updated_at
    end
  end

  describe "ingestion" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      start_supervised!({SourceSup, source})
      :timer.sleep(500)
      {:ok, source: source}
    end

    test "correctly retains the 100 items", %{source: source} do
      events = for _n <- 1..305, do: build(:log_event, source: source, some: "event")
      assert {:ok, 305} = Backends.ingest_logs(events, source)

      TestUtils.retry_assert(fn ->
        cached = Backends.list_recent_logs(source)
        assert length(cached) == 100
        cached = Backends.list_recent_logs_local(source)
        assert length(cached) == 100
      end)
    end

    test "bug: more than 100 items with dynamic pipelines", %{source: source} do
      events = for _n <- 1..305, do: build(:log_event, source: source, some: "event")
      # manually increase count of the dynamic pipielines
      name = Backends.via_source(source.id, Pipeline, nil)
      DynamicPipeline.add_pipeline(name)
      assert {:ok, 305} = Backends.ingest_logs(events, source)

      TestUtils.retry_assert(fn ->
        cached = Backends.list_recent_logs(source)
        assert length(cached) == 100
        cached = Backends.list_recent_logs_local(source)
        assert length(cached) == 100
      end)
    end

    test "caches latest timestamp correctly", %{source: source} do
      assert Backends.fetch_latest_timestamp(source) == 0
      le = build(:log_event, source: source, some: "event")
      assert {:ok, _} = Backends.ingest_logs([le], source)

      TestUtils.retry_assert(fn ->
        assert Backends.fetch_latest_timestamp(source) != 0
      end)
    end

    test "cache_estimated_buffer_lens/1 will cache all queue information", %{
      source: %{id: source_id} = source
    } do
      assert {:ok,
              %{
                len: 0,
                queues: [_, _]
              }} = Backends.cache_local_buffer_lens(source_id)

      events = for _n <- 1..5, do: build(:log_event, source: source, some: "event")
      assert {:ok, 5} = Backends.ingest_logs(events, source)

      assert {:ok,
              %{
                len: 5,
                queues: [_, _]
              }} = Backends.cache_local_buffer_lens(source_id)
    end

    test "emits telemetry events for backend ingestion", %{source: source} do
      user = source.user_id |> then(&Repo.get(User, &1))
      backend = insert(:backend, user: user, type: :postgres)
      source = Sources.preload_backends(source)
      {:ok, source} = Backends.update_source_backends(source, [backend])

      TestUtils.attach_forwarder([:logflare, :backends, :ingest, :dispatch])

      log_count = 119

      events = for _n <- 1..log_count, do: build(:log_event, source: source)
      assert {:ok, ^log_count} = Backends.ingest_logs(events, source)

      # for specific backend
      assert_receive {:telemetry_event, [:logflare, :backends, :ingest, :dispatch],
                      %{count: ^log_count}, %{backend_type: :postgres}}

      # for system default
      assert_receive {:telemetry_event, [:logflare, :backends, :ingest, :dispatch],
                      %{count: ^log_count}, %{backend_type: :bigquery}}
    end
  end

  describe "ingest filters" do
    setup do
      insert(:plan)
      [user: insert(:user)]
    end

    test "drop filter", %{user: user} do
      {:ok, lql_filters} = Lql.Parser.parse("testing", TestUtils.default_bq_schema())

      source =
        insert(:source, user: user, drop_lql_string: "testing", drop_lql_filters: lql_filters)

      start_supervised!({SourceSup, source})
      :timer.sleep(1000)

      le = build(:log_event, message: "testing 123", source: source)

      assert {:ok, 0} = Backends.ingest_logs([le], source)
      assert [] = Backends.list_recent_logs_local(source)
      :timer.sleep(1000)
    end

    test "route to source with lql", %{user: user} do
      [source, target] = insert_pair(:source, user: user)
      insert(:rule, lql_string: "testing", sink: target.token, source_id: source.id)
      source = Logflare.Repo.preload(source, :rules, force: true)
      start_supervised!({SourceSup, source}, id: :source)
      start_supervised!({SourceSup, target}, id: :target)
      :timer.sleep(500)

      assert {:ok, 2} =
               Backends.ingest_logs(
                 [
                   %{"message" => "some another"},
                   %{"message" => "some testing 123"}
                 ],
                 source
               )

      TestUtils.retry_assert(fn ->
        # 2 events
        assert Backends.list_recent_logs_local(source) |> length() == 2
        # 1 events
        assert Backends.list_recent_logs_local(target) |> length() == 1
      end)
    end

    test "routing depth is max 1 level (duplicate)", %{user: user} do
      [source, target] = insert_pair(:source, user: user)
      other_target = insert(:source, user: user)
      insert(:rule, lql_string: "testing", sink: target.token, source_id: source.id)
      insert(:rule, lql_string: "testing", sink: other_target.token, source_id: target.id)
      source = source |> Repo.preload(:rules, force: true)
      start_supervised!({SourceSup, source}, id: :source)
      start_supervised!({SourceSup, target}, id: :target)
      start_supervised!({SourceSup, other_target}, id: :other_target)
      :timer.sleep(500)

      assert {:ok, 1} = Backends.ingest_logs([%{"event_message" => "testing 123"}], source)

      TestUtils.retry_assert(fn ->
        assert Backends.list_recent_logs_local(source) |> length() == 1
        assert Backends.list_recent_logs_local(target) |> length() == 1
        assert Backends.list_recent_logs_local(other_target) |> length() == 0
      end)
    end

    test "routing depth is max 1 level", %{user: user} do
      [source, target] = insert_pair(:source, user: user)
      other_target = insert(:source, user: user)
      insert(:rule, lql_string: "testing", sink: target.token, source_id: source.id)
      insert(:rule, lql_string: "testing", sink: other_target.token, source_id: target.id)
      source = source |> Repo.preload(:rules, force: true)
      start_supervised!({SourceSup, source}, id: :source)
      start_supervised!({SourceSup, target}, id: :target)
      start_supervised!({SourceSup, other_target}, id: :other_target)
      :timer.sleep(500)

      assert {:ok, 1} = Backends.ingest_logs([%{"event_message" => "testing 123"}], source)

      TestUtils.retry_assert(fn ->
        # 1 events
        assert Backends.list_recent_logs_local(source) |> length() == 1
        # 1 events
        assert Backends.list_recent_logs_local(target) |> length() == 1
        # 0 events
        assert Backends.list_recent_logs_local(other_target) |> length() == 0
      end)
    end

    test "route to backend", %{user: user} do
      pid = self()
      ref = make_ref()

      Backends.Adaptor.WebhookAdaptor.Client
      |> expect(:send, 1, fn opts ->
        if length(opts[:body]) == 1 do
          send(pid, ref)
        else
          raise "ingesting more than 1 event"
        end

        {:ok, %Tesla.Env{}}
      end)

      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :webhook,
          config: %{url: "https://some-url.com"},
          user: user
        )

      insert(:rule, lql_string: "testing", backend: backend, source_id: source.id)
      source = source |> Repo.preload(:rules, force: true)
      start_supervised!({SourceSup, source}, id: :source)

      assert {:ok, 2} =
               Backends.ingest_logs(
                 [%{"event_message" => "testing 123"}, %{"event_message" => "not rounted"}],
                 source
               )

      TestUtils.retry_assert(fn ->
        assert_received ^ref
      end)

      :timer.sleep(1000)
    end

    test "cascade delete for rules on backend deletion", %{user: user} do
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :webhook,
          config: %{url: "https://some-url.com"},
          user: user
        )

      rule = insert(:rule, lql_string: "testing", backend: backend, source_id: source.id)
      Repo.delete(backend)
      refute Rules.get_rule(rule.id)
    end

    test "cascade delete for rules on source deletion", %{user: user} do
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :webhook,
          config: %{url: "https://some-url.com"},
          user: user
        )

      rule = insert(:rule, lql_string: "testing", backend: backend, source_id: source.id)
      Repo.delete(source)
      refute Rules.get_rule(rule.id)
    end
  end

  describe "handle_resolve_count/3" do
    test "resolve_count will increase counts when queue size is above threshold" do
      check all pipeline_count <- integer(0..100),
                queue_size <- integer(505..10_000),
                avg_rate <- integer(100..10_000),
                last <- member_of([nil, NaiveDateTime.utc_now()]) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: last,
          last_count_decrease: last
        }

        desired =
          Backends.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => 0,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired > pipeline_count
      end
    end

    test "resolve_count will increase counts when startup queue is non-empty" do
      check all pipeline_count <- integer(0..100),
                queue_size <- integer(1..250),
                startup_queue_size <- integer(5000..10_000),
                avg_rate <- integer(100..10_000),
                last <- member_of([nil, NaiveDateTime.utc_now()]) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: last,
          last_count_decrease: last
        }

        desired =
          Backends.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired - pipeline_count > 5
      end
    end

    test "resolve_count increases startup queue by 1 if less than 500 " do
      check all pipeline_count <- constant(0),
                startup_queue_size <- integer(1..444),
                avg_rate <- integer(1..500) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: NaiveDateTime.utc_now(),
          last_count_decrease: NaiveDateTime.utc_now()
        }

        desired =
          Backends.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size
            },
            avg_rate
          )

        assert desired - pipeline_count == 1
      end
    end

    test "resolve_count will decrease counts" do
      check all pipeline_count <- integer(2..100),
                queue_size <- integer(0..49),
                startup_queue_size <- constant(0),
                avg_rate <- integer(0..10_000),
                since <- integer(71..100) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: NaiveDateTime.utc_now(),
          last_count_decrease: NaiveDateTime.utc_now() |> NaiveDateTime.add(-since)
        }

        desired =
          Backends.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired < pipeline_count
        assert desired != 0
      end
    end

    test "resolve_count scale to zero" do
      check all pipeline_count <- constant(1),
                queue_size <- constant(0),
                startup_queue_size <- constant(0),
                avg_rate <- constant(0),
                since <- integer(360..1000) do
        state = %{
          pipeline_count: pipeline_count,
          max_pipelines: 101,
          last_count_increase: NaiveDateTime.utc_now(),
          last_count_decrease: NaiveDateTime.utc_now() |> NaiveDateTime.add(-since)
        }

        desired =
          Backends.handle_resolve_count(
            state,
            %{
              {1, 2, nil} => startup_queue_size,
              {1, 2, make_ref()} => queue_size
            },
            avg_rate
          )

        assert desired < pipeline_count
        assert desired == 0
      end
    end
  end

  describe "ingestion with backend" do
    setup :set_mimic_global

    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)

      insert(:backend,
        type: :webhook,
        sources: [source],
        config: %{url: "https://some-url.com"}
      )

      start_supervised!({SourceSup, source})
      :timer.sleep(500)
      {:ok, source: source}
    end

    test "backends receive dispatched log events", %{source: source} do
      pid = self()
      ref = make_ref()

      Backends.Adaptor.WebhookAdaptor.Client
      |> expect(:send, fn opts ->
        [event] = opts[:body]
        send(pid, {ref, event})
      end)

      event = build(:log_event, source: source, message: "some event")
      assert {:ok, 1} = Backends.ingest_logs([event], source)

      TestUtils.retry_assert(fn ->
        assert_received {^ref, %{"event_message" => "some event"}}
      end)

      :timer.sleep(1000)
    end
  end

  describe "benchmarks" do
    setup do
      insert(:plan)
      start_supervised!(BencheeAsync.Reporter)

      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 _dataset_id,
                                                 _table_name,
                                                 _opts ->
        BencheeAsync.Reporter.record()
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      user = insert(:user)
      [user: user]
    end

    # This benchmarks two areas:
    # - transformation of params to log events
    @tag :benchmark
    @tag :skip
    test "BQ - Backend ingestion", %{user: user} do
      [_source1, source2] = insert_pair(:source, user: user, rules: [])
      start_supervised!({SourceSup, source2})

      batch =
        for _i <- 1..150 do
          %{"message" => "some message"}
        end

      BencheeAsync.run(
        %{
          "SourceSup BQ with Backends.ingest_logs/2" => fn ->
            Backends.ingest_logs(batch, source2)
          end
        },
        time: 3,
        warmup: 1,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    # This benchmarks two areas:
    # - rules dispatching, with and without any rules
    @tag :benchmark
    @tag :skip
    test "backend rules routing benchmarking", %{user: user} do
      backend = insert(:backend, user: user)
      [source1, source2] = insert_pair(:source, user: user, rules: [])

      for _ <- 1..250 do
        insert(:rule, source: source2, backend: backend, lql_string: "message")
      end

      source2 = Sources.preload_defaults(source2)

      start_supervised!({SourceSup, source1}, id: :no_rules)
      start_supervised!({SourceSup, source2}, id: :with_rules)

      batch1 =
        for _i <- 1..250 do
          build(:log_event, source: source1, message: "some message")
        end

      batch2 =
        for _i <- 1..250 do
          build(:log_event, source: source2, message: "some message")
        end

      Benchee.run(
        %{
          "with rules" => fn ->
            SourceRouting.route_to_sinks_and_ingest(batch1, source1)
          end,
          "100 rules" => fn ->
            SourceRouting.route_to_sinks_and_ingest(batch2, source2)
          end
        },
        time: 3,
        warmup: 1,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end

  @tag :benchmark
  @tag timeout: :infinity
  @tag :skip
  # benchmark results:
  # using the buffers cache results in >5899.70x higher ips for 50k inputs
  # memory usage is 3.4x higher without cache
  # reductions is 3455x higher without cache
  test "local_pending_buffer_len" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    {:ok, tid} = IngestEventQueue.upsert_tid({source.id, backend.id, nil})
    sb = {source.id, backend.id}

    Benchee.run(
      %{
        "with cache" => fn {_input, _resource} ->
          Backends.cached_local_pending_buffer_len(source, backend)
        end
      },
      inputs: %{
        "50k" => for(_ <- 1..50_000, do: build(:log_event)),
        "10k" => for(_ <- 1..10_000, do: build(:log_event)),
        "1k" => for(_ <- 1..1_000, do: build(:log_event))
      },
      # insert the batch
      before_scenario: fn input ->
        :ets.delete_all_objects(tid)
        IngestEventQueue.add_to_table(sb, input)

        PubSubRates.Cache.cache_buffers(source.id, backend.id, %{
          Node.self() => %{len: length(input)}
        })

        {input, nil}
      end,
      time: 3,
      warmup: 1,
      memory_time: 3,
      reduction_time: 3,
      print: [configuration: false]
    )
  end

  describe "sync_backend_across_cluster/1" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      {:ok, user: user, source: source}
    end

    test "syncs backend across cluster for all associated sources", %{
      source: source,
      user: user
    } do
      # Start actual SourceSup process
      start_supervised!({SourceSup, source})
      :timer.sleep(500)

      via = Backends.via_source(source, SourceSup)

      children =
        Supervisor.which_children(via)
        |> Enum.filter(fn
          {{mod, _, _}, _pid, _type, _sup} -> mod == Backends.Adaptor.WebhookAdaptor
          _ -> false
        end)

      assert Enum.empty?(children)

      backend =
        insert(:backend,
          user: user,
          sources: [source],
          type: :webhook,
          config: %{url: "http://test.com"}
        )

      # Mock the cluster RPC calls
      Logflare.Cluster.Utils
      |> expect(:rpc_multicast, 1, fn
        Backends, :sync_backends_local, [%_{}, [_]] = args ->
          apply(Backends, :sync_backends_local, args)
      end)

      assert :ok = Backends.sync_backend_across_cluster(backend.id)

      via = Backends.via_source(source, SourceSup)

      children =
        Supervisor.which_children(via)
        |> Enum.filter(fn
          {{mod, _, _}, _pid, _type, _sup} -> mod == Backends.Adaptor.WebhookAdaptor
          _ -> false
        end)

      assert length(children) == 1
    end

    test "handles non-existent backend gracefully" do
      non_existent_id = 99_999

      reject(&Logflare.Cluster.Utils.rpc_multicast/3)

      assert :ok = Backends.sync_backend_across_cluster(non_existent_id)
    end

    test "works with backend having no associated sources", %{user: user} do
      # Backend has no associated sources
      backend =
        insert(:backend,
          user: user,
          sources: [],
          default_ingest?: true,
          type: :webhook,
          config: %{url: "http://test.com"}
        )

      # Should not make any RPC calls

      reject(&Logflare.Cluster.Utils.rpc_multicast/3)

      assert :ok = Backends.sync_backend_across_cluster(backend.id)
    end
  end
end
