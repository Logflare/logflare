defmodule Logflare.BackendsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSup
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Source.RecentLogsServer
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Source.ChannelTopics
  alias Logflare.Lql
  alias Logflare.Logs
  alias Logflare.Source.V1SourceSup
  alias Logflare.PubSubRates
  alias Logflare.Logs.SourceRouting

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "backend management" do
    setup do
      user = insert(:user)
      [source: insert(:source, user_id: user.id), user: user]
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
      assert [] = Backends.list_backends(source)
      assert {:ok, %Source{}} = Backends.update_source_backends(source, [backend1, backend2])
      assert [_, _] = Backends.list_backends(source)

      # removal
      assert {:ok, %Source{}} = Backends.update_source_backends(source, [])
      assert [] = Backends.list_backends(source)
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

  describe "SourceSup management" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source, user: user}
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

    test "source_sup_started?/1, lookup/2", %{source: source} do
      assert false == Backends.source_sup_started?(source)
      start_supervised!({SourceSup, source})
      :timer.sleep(1000)
      assert true == Backends.source_sup_started?(source)
      assert {:ok, _pid} = Backends.lookup(RecentLogsServer, source.token)
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
      events = for _n <- 1..105, do: build(:log_event, source: source, some: "event")
      assert {:ok, 105} = Backends.ingest_logs(events, source)
      :timer.sleep(1500)
      cached = Backends.list_recent_logs(source)
      assert length(cached) == 100
      cached = Backends.list_recent_logs_local(source)
      assert length(cached) == 100
    end

    test "performs broadcasts for global cache rates and dashboard rates", %{
      source: %{token: source_token} = source
    } do
      PubSubRates.subscribe(:all)
      ChannelTopics.subscribe_dashboard(source_token)
      ChannelTopics.subscribe_source(source_token)
      le = build(:log_event, source: source)
      assert {:ok, 1} = Backends.ingest_logs([le], source)
      :timer.sleep(1000)

      TestUtils.retry_assert(fn ->
        assert_received %_{event: "rate", payload: %{rate: _}}
        # broadcast for recent logs page
        assert_received %_{event: _, payload: %{body: %{}}}
      end)
    end

    test "can get length of queue", %{source: source} do
      assert Backends.local_pending_buffer_len(source, nil) == 0
      assert Backends.local_pending_buffer_len(source) == 0
      events = for _n <- 1..5, do: build(:log_event, source: source, some: "event")
      assert {:ok, 5} = Backends.ingest_logs(events, source)
      assert Backends.local_pending_buffer_len(source) > 0
      # Producer will pop from the queue
      :timer.sleep(1_500)
      assert Backends.local_pending_buffer_len(source) == 0
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
      # only the init message in RLS
      assert [_] = Backends.list_recent_logs_local(source)
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

      :timer.sleep(1000)
      # init message + 2 events
      assert Backends.list_recent_logs_local(source) |> length() == 3
      # init message + 1 events
      assert Backends.list_recent_logs_local(target) |> length() == 2
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
      :timer.sleep(1000)
      # init message + 1 events
      assert Backends.list_recent_logs_local(source) |> length() == 2
      # init message + 1 events
      assert Backends.list_recent_logs_local(target) |> length() == 2
      # init message + 0 events
      assert Backends.list_recent_logs_local(other_target) |> length() == 1
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

      assert_receive ^ref, 2_000
      :timer.sleep(1000)
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

  test "broadcast_buffer_lens broadcasts every n ms" do
    PubSubRates.subscribe(:buffers)
    user = insert(:user)
    %{token: token} = source = insert(:source, user: user)
    assert :ok = Backends.broadcast_buffer_lens(source.id, nil, %{"some-token": 2})
    :timer.sleep(300)

    assert_receive {:buffers, ^token, _payload}
    assert PubSubRates.Cache.get_cluster_buffers(token) == 2
  end

  test "BufferProducer broadcasts every n seconds with backend differentiation" do
    PubSubRates.subscribe(:buffers)
    user = insert(:user)
    %{token: source_token} = source = insert(:source, user: user)
    %{token: backend_token} = backend = insert(:backend, user: user)

    assert :ok = Backends.broadcast_buffer_lens(source.id, backend.id, %{"some-token": 55})
    :timer.sleep(200)
    assert_receive {:buffers, ^source_token, ^backend_token, _payload}

    assert PubSubRates.Cache.get_cluster_buffers(source_token, backend_token) == 55
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
    # - BQ max insertion rate
    @tag :benchmark
    test "BQ - v1 Logs vs v2 Logs vs v2 Backend", %{user: user} do
      [source1, source2] = insert_pair(:source, user: user, rules: [])
      # start_supervised!({Pipeline, [rls, name: @pipeline_name]})
      start_supervised!({V1SourceSup, source: source1})
      start_supervised!({SourceSup, source2})

      batch =
        for _i <- 1..150 do
          %{"message" => "some message"}
        end

      BencheeAsync.run(
        %{
          "v1SourceSup BQ with Logs.ingest_logs/2" => fn ->
            Logs.ingest_logs(batch, source1)
          end,
          "SourceSup v2 BQ with Logs.ingest_logs/2" => fn ->
            Logs.ingest_logs(batch, source2)
          end,
          "SourceSup v2 BQ with Backends.ingest_logs/2" => fn ->
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
    test "backend rules routing benchmarking", %{user: user} do
      backend = insert(:backend, user: user)
      [source1, source2] = insert_pair(:source, user: user, rules: [])

      for _ <- 1..250 do
        insert(:rule, source: source2, backend: backend, lql_string: "message")
      end

      source2 = Sources.preload_defaults(source2)

      # start_supervised!({Pipeline, [rls, name: @pipeline_name]})
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
            SourceRouting.route_to_sinks_and_ingest(batch1)
          end,
          "100 rules" => fn ->
            SourceRouting.route_to_sinks_and_ingest(batch2)
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
end
