defmodule Logflare.Backends.WebhookAdaptorTest do
  @moduledoc false
  use Logflare.DataCase

  doctest Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Backends.SourceSup
  @subject Logflare.Backends.Adaptor.WebhookAdaptor

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)
    :ok
  end

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

      start_supervised!({SourceSup, source})
      :timer.sleep(500)
      [source: source, backend: backend]
    end

    test "ingest", %{source: source} do
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

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive ^ref, 2000
    end

    test "uses cache for config fetching", %{source: source} do
      Logflare.Repo.update_all(Backend,
        set: [config_encrypted: %{http: "http1", url: "https://other-email.com"}]
      )

      this = self()
      ref = make_ref()

      @subject.Client
      |> expect(:send, fn req ->
        assert req[:url] =~ "other-email"
        body = req[:body]
        assert is_list(body)
        send(this, ref)
        %Tesla.Env{}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive ^ref, 2000
    end
  end

  describe "test_connection/1" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :webhook,
          sources: [source],
          config: %{http: "http1", url: "https://example.com", headers: %{"x-key" => "v"}}
        )

      [backend: backend]
    end

    test "succeeds on 2xx response", %{backend: backend} do
      @subject.Client
      |> expect(:send, fn req ->
        assert req[:url] == "https://example.com"
        assert req[:body] == []
        assert req[:headers] == %{"x-key" => "v"}
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      assert :ok = @subject.test_connection(backend)
    end

    test "returns error on non-2xx response", %{backend: backend} do
      @subject.Client
      |> expect(:send, fn _req ->
        {:ok, %Tesla.Env{status: 401, body: %{"error" => "unauthorized"}}}
      end)

      assert {:error, reason} = @subject.test_connection(backend)
      assert reason =~ "401"
    end

    test "returns error on transport failure", %{backend: backend} do
      @subject.Client
      |> expect(:send, fn _req -> {:error, :nxdomain} end)

      assert {:error, reason} = @subject.test_connection(backend)
      assert reason =~ "nxdomain"
    end
  end

  describe "test_connection/2" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :webhook,
          sources: [source],
          config: %{http: "http1", url: "https://example.com"}
        )

      [backend: backend]
    end

    test "forwards a custom probe body", %{backend: backend} do
      probe = %{streams: []}

      @subject.Client
      |> expect(:send, fn req ->
        assert req[:body] == probe
        {:ok, %Tesla.Env{status: 204, body: ""}}
      end)

      assert :ok = @subject.test_connection(backend, probe)
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

  test "cast_and_validate_config/1 for gzip" do
    assert %Ecto.Changeset{
             valid?: true,
             changes: %{
               gzip: true
             }
           } =
             Adaptor.cast_and_validate_config(@subject, %{url: "http://example.com", gzip: true})

    assert %Ecto.Changeset{
             valid?: true,
             changes: %{
               gzip: false
             }
           } =
             Adaptor.cast_and_validate_config(@subject, %{url: "http://example.com", gzip: false})
  end

  test "cast_and_validate_config/1 for http" do
    assert %Ecto.Changeset{
             valid?: true,
             changes: %{
               gzip: true,
               http: "http1"
             }
           } =
             Adaptor.cast_and_validate_config(@subject, %{
               url: "http://example.com",
               http: "http1"
             })

    assert %Ecto.Changeset{
             valid?: true,
             changes: %{
               gzip: true,
               http: "http2"
             }
           } =
             Adaptor.cast_and_validate_config(@subject, %{
               url: "http://example.com",
               http: "http2"
             })
  end

  describe "redact_config/1" do
    test "redacts Authorization header" do
      config = %{
        headers: %{
          "Authorization" => "Bearer secret-token-123",
          "Content-Type" => "application/json"
        }
      }

      assert %{headers: %{"Authorization" => "REDACTED", "Content-Type" => "application/json"}} =
               @subject.redact_config(config)
    end

    test "redacts authorization header case-insensitive" do
      config = %{headers: %{"authorization" => "Basic dXNlcjpwYXNz"}}
      assert %{headers: %{"authorization" => "REDACTED"}} = @subject.redact_config(config)
    end
  end

  describe "benchmark" do
    @describetag :benchmark

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

      start_supervised!({SourceSup, source})

      [backend: backend, source: source]
    end

    test "defaults", %{backend: backend, source: source} do
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
            Backends.ingest_logs(batch_1, source, backend)
          end,
          "batch-10" => fn ->
            Backends.ingest_logs(batch_10, source, backend)
          end,
          "batch-100" => fn ->
            Backends.ingest_logs(batch_100, source, backend)
          end,
          "batch-250" => fn ->
            Backends.ingest_logs(batch_250, source, backend)
          end,
          "batch-500" => fn ->
            Backends.ingest_logs(batch_500, source, backend)
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
