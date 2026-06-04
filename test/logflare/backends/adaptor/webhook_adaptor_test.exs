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

  describe "cast_and_validate_config/1 SSRF protection" do
    @ssrf_error {"URL must not target private or reserved IP addresses", [validation: :ssrf]}

    test "rejects private/reserved IP addresses" do
      blocked = [
        # loopback
        "http://127.0.0.1/",
        "http://127.1.2.3/",
        # RFC1918
        "http://10.0.0.1/",
        "http://172.16.0.1/",
        "http://172.31.255.255/",
        "http://192.168.1.1/",
        # link-local / cloud metadata
        "http://169.254.169.254/latest/meta-data/",
        # all-zeros, CGNAT
        "http://0.0.0.0/",
        "http://100.64.0.1/",
        # private IPv6
        "http://[::1]/",
        "http://[fe80::1]/",
        "http://[fc00::1]/",
        "http://[fd00::1]/"
      ]

      for url <- blocked do
        cs = Adaptor.cast_and_validate_config(@subject, %{url: url})
        assert cs.errors[:url] == @ssrf_error, "expected SSRF block for #{url}"
      end
    end

    test "allows public IP addresses (172.16.0.0/12 boundary)" do
      for url <- ["http://172.15.0.1/", "http://172.32.0.1/"] do
        assert %Ecto.Changeset{valid?: true} =
                 Adaptor.cast_and_validate_config(@subject, %{url: url}),
               "expected valid for #{url}"
      end
    end

    test "rejects hostname resolving to loopback" do
      cs = Adaptor.cast_and_validate_config(@subject, %{url: "http://localhost/"})
      assert %Ecto.Changeset{valid?: false} = cs
      assert cs.errors[:url] != []
    end
  end

  describe "SSRF middleware integration" do
    test "Client.send/1 blocks private IPs at request time" do
      # Call Client.send/1 directly without mocking to verify SSRFProtection is
      # wired into the Tesla client stack. SSRFProtection runs before Finch, so
      # private IPs are rejected without making a real network connection.
      for url <- [
            "http://127.0.0.1/metrics",
            "http://169.254.169.254/latest/meta-data/",
            "http://10.0.0.1/",
            "http://192.168.1.1/"
          ] do
        assert {:error, _reason} =
                 @subject.Client.send(url: url, body: []),
               "expected SSRF block for #{url}"
      end
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

  describe "cast_config/2 header redaction round-trip" do
    @existing %{
      url: "https://example.com",
      headers: %{"Authorization" => "Bearer secret-token-123", "Content-Type" => "application/json"}
    }

    test "restores the stored secret when the REDACTED sentinel is submitted back" do
      params = %{
        url: "https://example.com",
        headers: %{"Authorization" => "REDACTED", "Content-Type" => "application/json"}
      }

      changeset = @subject.cast_config(params, @existing)

      assert Ecto.Changeset.get_field(changeset, :headers) == %{
               "Authorization" => "Bearer secret-token-123",
               "Content-Type" => "application/json"
             }
    end

    test "preserves the stored secret while adding a new header" do
      params = %{
        url: "https://example.com",
        headers: %{
          "Authorization" => "REDACTED",
          "Content-Type" => "application/json",
          "x-custom" => "new-value"
        }
      }

      changeset = @subject.cast_config(params, @existing)

      assert Ecto.Changeset.get_field(changeset, :headers) == %{
               "Authorization" => "Bearer secret-token-123",
               "Content-Type" => "application/json",
               "x-custom" => "new-value"
             }
    end

    test "applies a new Authorization value when the user changes it" do
      params = %{
        url: "https://example.com",
        headers: %{"Authorization" => "Bearer new-token-456"}
      }

      changeset = @subject.cast_config(params, @existing)

      assert Ecto.Changeset.get_field(changeset, :headers) == %{
               "Authorization" => "Bearer new-token-456"
             }
    end

    test "clears headers when an empty map is submitted" do
      params = %{url: "https://example.com", headers: %{}}

      changeset = @subject.cast_config(params, @existing)

      assert Ecto.Changeset.get_field(changeset, :headers) == %{}
    end

    test "keeps existing headers when none are submitted" do
      params = %{url: "https://example.com"}

      changeset = @subject.cast_config(params, @existing)

      assert Ecto.Changeset.get_field(changeset, :headers) == @existing.headers
    end

    test "drops a sentinel with no stored value to restore" do
      params = %{
        url: "https://example.com",
        headers: %{"Authorization" => "REDACTED"}
      }

      changeset = @subject.cast_config(params, %{url: "https://example.com", headers: %{}})

      assert Ecto.Changeset.get_field(changeset, :headers) == %{}
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
