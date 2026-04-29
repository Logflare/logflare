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

  describe "cast_and_validate_config/1 SSRF protection" do
    test "rejects loopback IPv4 addresses" do
      for url <- [
            "http://127.0.0.1/endpoint",
            "http://127.1.2.3/endpoint",
            "https://127.0.0.1:8080/metrics"
          ] do
        cs = Adaptor.cast_and_validate_config(@subject, %{url: url})
        assert %Ecto.Changeset{valid?: false} = cs, "expected invalid for #{url}"

        assert {"URL must not target private or reserved IP addresses", [validation: :ssrf]} in cs.errors[
                 :url
               ],
               "expected SSRF error for #{url}"
      end
    end

    test "rejects RFC1918 private IPv4 addresses" do
      for url <- [
            "http://10.0.0.1/internal",
            "http://10.255.255.255/internal",
            "http://172.16.0.1/internal",
            "http://172.31.255.255/internal",
            "http://192.168.1.1/internal",
            "http://192.168.0.0/internal"
          ] do
        cs = Adaptor.cast_and_validate_config(@subject, %{url: url})
        assert %Ecto.Changeset{valid?: false} = cs, "expected invalid for #{url}"

        assert {"URL must not target private or reserved IP addresses", [validation: :ssrf]} in cs.errors[
                 :url
               ],
               "expected SSRF error for #{url}"
      end
    end

    test "rejects link-local and other reserved IPv4 addresses" do
      for url <- [
            "http://169.254.169.254/latest/meta-data/",
            "http://0.0.0.0/endpoint",
            "http://100.64.0.1/endpoint",
            "http://100.127.255.255/endpoint"
          ] do
        cs = Adaptor.cast_and_validate_config(@subject, %{url: url})
        assert %Ecto.Changeset{valid?: false} = cs, "expected invalid for #{url}"

        assert {"URL must not target private or reserved IP addresses", [validation: :ssrf]} in cs.errors[
                 :url
               ],
               "expected SSRF error for #{url}"
      end
    end

    test "rejects private IPv6 addresses" do
      for url <- [
            "http://[::1]/endpoint",
            "http://[fe80::1]/endpoint",
            "http://[fc00::1]/endpoint",
            "http://[fd00::1]/endpoint"
          ] do
        cs = Adaptor.cast_and_validate_config(@subject, %{url: url})
        assert %Ecto.Changeset{valid?: false} = cs, "expected invalid for #{url}"

        assert {"URL must not target private or reserved IP addresses", [validation: :ssrf]} in cs.errors[
                 :url
               ],
               "expected SSRF error for #{url}"
      end
    end

    test "rejects hostname that resolves to loopback (localhost)" do
      cs = Adaptor.cast_and_validate_config(@subject, %{url: "http://localhost/endpoint"})
      assert %Ecto.Changeset{valid?: false} = cs
      assert cs.errors[:url] != []
    end

    test "rejects 172.16.0.0/12 boundary correctly" do
      # 172.15.x.x is NOT in the RFC1918 range — literal IP, no DNS
      cs_outside = Adaptor.cast_and_validate_config(@subject, %{url: "http://172.15.0.1/endpoint"})
      assert %Ecto.Changeset{valid?: true} = cs_outside

      # 172.32.x.x is NOT in the RFC1918 range — literal IP, no DNS
      cs_outside2 = Adaptor.cast_and_validate_config(@subject, %{url: "http://172.32.0.1/endpoint"})
      assert %Ecto.Changeset{valid?: true} = cs_outside2

      # 172.16.x.x IS in the RFC1918 range
      cs_inside = Adaptor.cast_and_validate_config(@subject, %{url: "http://172.16.0.1/endpoint"})
      assert %Ecto.Changeset{valid?: false} = cs_inside
      assert {"URL must not target private or reserved IP addresses", [validation: :ssrf]} in cs_inside.errors[:url]
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
