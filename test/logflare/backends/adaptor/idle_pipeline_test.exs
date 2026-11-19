defmodule Logflare.Backends.Adaptor.IdlePipelineTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.SourceSup
  alias Logflare.ShutdownDrain
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Tesla.MockAdapter

  describe "resolve_idle_count/4" do
    test "pending events wake an idle pipeline regardless of the activity timestamp" do
      assert Backends.resolve_idle_count(%{pipeline_count: 0}, 1, nil, 200) == 1
      assert Backends.resolve_idle_count(%{pipeline_count: 1}, 1, 0, 200) == 1
    end

    test "an empty pipeline stays at zero or waits until its idle window has elapsed" do
      now = System.monotonic_time(:millisecond)

      assert Backends.resolve_idle_count(%{pipeline_count: 0}, 0, nil, 1_000) == 0
      assert Backends.resolve_idle_count(%{pipeline_count: 1}, 0, nil, 1_000) == 1
      assert Backends.resolve_idle_count(%{pipeline_count: 1}, 0, now, 1_000) == 1
      assert Backends.resolve_idle_count(%{pipeline_count: 1}, 0, now - 1_000, 1_000) == 0
    end
  end

  describe "enabled adaptors" do
    setup %{group: group} do
      previous = Application.fetch_env!(:logflare, :scale_to_zero_adaptors)
      previous_idle_ms = Application.fetch_env!(:logflare, :scale_to_zero_idle_ms)
      Application.put_env(:logflare, :scale_to_zero_adaptors, [group])
      Application.put_env(:logflare, :scale_to_zero_idle_ms, 200)

      on_exit(fn ->
        Application.put_env(:logflare, :scale_to_zero_adaptors, previous)
        Application.put_env(:logflare, :scale_to_zero_idle_ms, previous_idle_ms)
      end)

      stub(Logflare.Google.BigQuery, :get_table, fn _ -> {:error, :not_found} end)
      start_supervised!(AllLogsLogged)
      insert(:plan)
      source = insert(:source, user: insert(:user))
      {backend, pipeline} = insert_backend(group, source)
      start_supervised!({SourceSup, source})

      [source: source, backend: backend, name: Backends.via_source(source, pipeline, backend)]
    end

    for group <- [:webhook, :http_based] do
      @tag group: group
      test "#{group} stops when idle and delivers a burst with one shard", %{
        group: group,
        name: name,
        source: source,
        backend: backend
      } do
        assert [shard] = DynamicPipeline.list_pipelines(name)
        assert {:error, :max_pipelines} = DynamicPipeline.add_pipeline(name)
        assert is_integer(IngestEventQueue.last_active({source.id, backend.id}))
        producer = shard |> Broadway.producer_names() |> hd() |> GenServer.whereis()
        producer_ref = Process.monitor(producer)

        assert_receive {:DOWN, ^producer_ref, :process, ^producer, _}, 2_500

        TestUtils.retry_assert(fn ->
          assert DynamicPipeline.pipeline_count(name) == 0
          assert DynamicPipeline.whereis(shard) == nil
        end)

        event_name = [:logflare, :backends, :dynamic_pipeline, :increment]
        increment_ref = :telemetry_test.attach_event_handlers(self(), [event_name])
        backend_id = backend.id
        stub_delivery(group, self())

        events = build_list(20, :log_event, source: source)
        assert {:ok, _} = Backends.ingest_logs(events, source)

        assert_receive {^event_name, ^increment_ref,
                        %{from_pipeline_count: 0, success_count: 1, error_count: 0},
                        %{backend_id: ^backend_id}},
                       2_500

        for event <- events do
          id = event.id
          assert_receive {:delivered, ^id}, 2_500
        end
      end
    end

    @tag group: :webhook
    test "shutdown drain wakes a webhook pipeline at zero", %{
      name: name,
      source: source,
      backend: backend
    } do
      TestUtils.retry_assert(fn -> assert DynamicPipeline.pipeline_count(name) == 0 end)
      stub_delivery(:webhook, self())

      event = build(:log_event, source: source)
      coordinator = DynamicPipeline.find_coordinator_name(name)
      :ok = :sys.suspend(coordinator)

      try do
        assert {:ok, _} = Backends.ingest_logs([event], source)
        assert IngestEventQueue.total_pending({source.id, backend.id, nil}) == 1
      after
        :ok = :sys.resume(coordinator)
      end

      now = System.monotonic_time(:millisecond)
      budget = %{queues: {now + 2_000, 2_000}, sync: nil, cleanup: {now + 3_000, 1_000}}

      assert ShutdownDrain.run(budget, :ok, nil, poll_interval_ms: 10) == :drained
      assert_receive {:delivered, id}, 1_000
      assert id == event.id
    end
  end

  defp insert_backend(:webhook, source) do
    backend =
      insert(:backend,
        type: :webhook,
        sources: [source],
        config: %{http: "http1", url: "https://example.com"}
      )

    {backend, WebhookAdaptor.Pipeline}
  end

  defp insert_backend(:http_based, source) do
    backend =
      insert(:backend,
        type: :axiom,
        sources: [source],
        config: %{api_token: "API-KEY", dataset_name: "logflare", domain: "api.axiom.co"}
      )

    {backend, HttpBased.Pipeline}
  end

  defp stub_delivery(:webhook, parent) do
    stub(WebhookAdaptor.Client, :send, fn req ->
      for body <- req[:body], do: send(parent, {:delivered, body["id"]})
      %Tesla.Env{status: 200}
    end)
  end

  defp stub_delivery(:http_based, parent) do
    adapter = fn env ->
      for body <- env.body |> :zlib.gunzip() |> Jason.decode!() do
        send(parent, {:delivered, body["id"]})
      end

      {:ok, %Tesla.Env{status: 200, body: ""}}
    end

    stub(HttpBased.Client, :new, fn opts ->
      HttpBased.Client
      |> Mimic.call_original(:new, [opts])
      |> MockAdapter.replace(adapter)
    end)
  end
end
