defmodule Logflare.Backends.WebhookAdaptorTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Backends.Adaptor

  @subject Logflare.Backends.Adaptor.WebhookAdaptor

  describe "ingestion tests" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :webhook,
          sources: [source],
          config: %{http: "http1", url: "https://example.com"}
        )

      pid = start_supervised!({@subject, {source, backend}})
      :timer.sleep(500)
      [pid: pid, backend: backend, source: source]
    end

    test "ingest/2", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      @subject.Client
      |> expect(:send, fn req ->
        body = req[:body]
        assert is_list(body)
        send(this, ref)
        %Tesla.Env{}
      end)

      le = build(:log_event, source: source)

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
      assert_receive ^ref, 2000
    end
  end

  test "cast_and_validate_config/1" do
    for valid <- [
          %{url: "http://example.com"},
          %{url: "https://example.com"}
        ] do
      assert %Ecto.Changeset{valid?: true} = Adaptor.cast_and_validate_config(@subject, valid),
             "valid: #{inspect(valid)}"
    end

    for invalid <- [
          %{},
          %{url: nil},
          %{url: "htp://invalid.com"}
        ] do
      assert %Ecto.Changeset{valid?: false} = Adaptor.cast_and_validate_config(@subject, invalid),
             "invalid: #{inspect(invalid)}"
    end
  end

  @tag :benchmark
  describe "benchmark" do
    setup do
      start_supervised!(BencheeAsync.Reporter)

      @subject.Client
      |> stub(:send, fn batch ->
        body = batch[:body]
        n = Enum.count(body)
        BencheeAsync.Reporter.record(n)
        # simulate latency
        :timer.sleep(100)
        %Tesla.Env{}
      end)

      user = insert(:user)
      source = insert(:source, user_id: user.id)

      backend =
        insert(:backend, type: :webhook, sources: [source], config: %{url: "https://example.com"})

      pid = start_supervised!({@subject, {source, backend}})

      [backend: backend, pid: pid, source: source]
    end

    test "defaults", %{pid: pid, backend: backend, source: source} do
      le = build(:log_event, source: source)
      batch_1 = [le]

      batch_10 =
        for _i <- 1..10 do
          le
        end

      batch_100 =
        for _i <- 1..100 do
          le
        end

      batch_250 =
        for _i <- 1..250 do
          le
        end

      batch_500 =
        for _i <- 1..500 do
          le
        end

      BencheeAsync.run(
        %{
          "batch-1" => fn ->
            @subject.ingest(pid, batch_1, source_id: source.id, backend_id: backend.id)
          end,
          "batch-10" => fn ->
            @subject.ingest(pid, batch_10, source_id: source.id, backend_id: backend.id)
          end,
          "batch-100" => fn ->
            @subject.ingest(pid, batch_100, source_id: source.id, backend_id: backend.id)
          end,
          "batch-250" => fn ->
            @subject.ingest(pid, batch_250, source_id: source.id, backend_id: backend.id)
          end,
          "batch-500" => fn ->
            @subject.ingest(pid, batch_500, source_id: source.id, backend_id: backend.id)
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
