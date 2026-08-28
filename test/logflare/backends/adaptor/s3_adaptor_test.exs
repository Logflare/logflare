defmodule Logflare.Backends.Adaptor.S3AdaptorTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.S3Adaptor

  doctest S3Adaptor

  @valid_config %{
    s3_bucket: "my-bucket",
    storage_region: "us-east-1",
    access_key_id: "AKID",
    secret_access_key: "SECRET",
    batch_timeout: 1_000
  }

  describe "validate_config/1 endpoint allowlist (check enabled, default)" do
    setup do
      Application.delete_env(:logflare, :unsafe_disable_ssrf_s3_endpoint_check)
      on_exit(fn -> Application.delete_env(:logflare, :unsafe_disable_ssrf_s3_endpoint_check) end)
    end

    test "allows nil endpoint (default AWS S3)" do
      assert %Ecto.Changeset{valid?: true} =
               Adaptor.cast_and_validate_config(S3Adaptor, @valid_config)
    end

    test "allows allowlisted AWS endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://bucket.s3.amazonaws.com")
        )

      assert %Ecto.Changeset{valid?: true} = cs
    end

    test "allows allowlisted Google Cloud Storage endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://storage.googleapis.com")
        )

      assert %Ecto.Changeset{valid?: true} = cs
    end

    test "allows allowlisted Cloudflare R2 endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://account-id.r2.cloudflarestorage.com")
        )

      assert %Ecto.Changeset{valid?: true} = cs
    end

    test "allows allowlisted Backblaze B2 endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://s3.us-west-000.backblazeb2.com")
        )

      assert %Ecto.Changeset{valid?: true} = cs
    end

    test "allows allowlisted DigitalOcean Spaces endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://nyc3.digitaloceanspaces.com")
        )

      assert %Ecto.Changeset{valid?: true} = cs
    end

    test "rejects non-allowlisted public endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://evil.example.com")
        )

      assert %Ecto.Changeset{valid?: false} = cs
      assert {_message, [validation: :endpoint_not_allowed]} = cs.errors[:endpoint]
    end

    test "rejects private/rebind-style endpoint not on allowlist" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://my-minio.internal")
        )

      assert %Ecto.Changeset{valid?: false} = cs
      assert {_message, [validation: :endpoint_not_allowed]} = cs.errors[:endpoint]
    end

    test "rejects an endpoint host containing an embedded space, even with a trusted suffix" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(
            @valid_config,
            :endpoint,
            "https://drain-3fq9k2z-mybucket .s3.ap-northeast-1.amazonaws.com"
          )
        )

      assert %Ecto.Changeset{valid?: false} = cs
      assert {_message, [validation: :endpoint_malformed]} = cs.errors[:endpoint]
    end

    test "rejects an endpoint whose host contains other whitespace characters" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://bucket\t.s3.amazonaws.com")
        )

      assert %Ecto.Changeset{valid?: false} = cs
      assert {_message, [validation: :endpoint_malformed]} = cs.errors[:endpoint]
    end

    test "rejects an endpoint with leading or trailing whitespace" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, " https://bucket.s3.amazonaws.com ")
        )

      assert %Ecto.Changeset{valid?: false} = cs
      assert {_message, [validation: :endpoint_malformed]} = cs.errors[:endpoint]
    end
  end

  describe "validate_config/1 endpoint allowlist (check disabled via flag)" do
    setup do
      Application.put_env(:logflare, :unsafe_disable_ssrf_s3_endpoint_check, true)
      on_exit(fn -> Application.delete_env(:logflare, :unsafe_disable_ssrf_s3_endpoint_check) end)
    end

    test "allows nil endpoint (default AWS S3)" do
      assert %Ecto.Changeset{valid?: true} =
               Adaptor.cast_and_validate_config(S3Adaptor, @valid_config)
    end

    test "allows arbitrary internal endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://my-minio.internal")
        )

      assert %Ecto.Changeset{valid?: true} = cs
    end

    test "allows any public non-allowlisted endpoint" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "https://custom-s3-provider.example.com")
        )

      assert %Ecto.Changeset{valid?: true} = cs
    end
  end

  describe "redact_config/1" do
    test "redacts secret_access_key when present" do
      config = %{secret_access_key: "secret-key-123", bucket_name: "my-bucket"}
      assert %{secret_access_key: "REDACTED"} = S3Adaptor.redact_config(config)
    end
  end

  describe "test_connection/1" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :s3,
          sources: [source],
          config: %{
            s3_bucket: "my-bucket",
            storage_region: "us-east-1",
            access_key_id: "AKID",
            secret_access_key: "SECRET",
            batch_timeout: 1_000
          }
        )

      [backend: backend]
    end

    test "uploads a sentinel parquet to the fixed probe key via PutObject", %{backend: backend} do
      this = self()
      ref = make_ref()

      ExAws
      |> expect(:request, fn op, opts ->
        send(this, {ref, op, opts})
        {:ok, %{status_code: 200}}
      end)

      assert :ok = S3Adaptor.test_connection(backend)
      assert_received {^ref, op, opts}

      assert %ExAws.Operation.S3{
               http_method: :put,
               bucket: "my-bucket",
               path: "_connection_test.parquet",
               body: body,
               headers: %{"content-type" => "application/vnd.apache.parquet"}
             } = op

      assert is_binary(body)
      assert String.starts_with?(body, "PAR1")
      assert opts[:access_key_id] == "AKID"
      assert opts[:secret_access_key] == "SECRET"
      assert opts[:region] == "us-east-1"
    end

    test "returns error when the upload fails", %{backend: backend} do
      ExAws
      |> expect(:request, fn _op, _opts ->
        {:error, {:http_error, 403, %{body: "AccessDenied"}}}
      end)

      assert {:error, reason} = S3Adaptor.test_connection(backend)
      assert reason =~ "AccessDenied"
    end
  end

  describe "push_log_events_to_s3/2" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :s3,
          sources: [source],
          config: %{
            s3_bucket: "my-bucket",
            storage_region: "us-east-1",
            access_key_id: "AKID",
            secret_access_key: "SECRET",
            batch_timeout: 1_000
          }
        )

      events = [build(:log_event, source: source)]

      [source: source, backend: backend, events: events]
    end

    test "uploads a parquet file keyed by normalized source token and timestamp", %{
      source: source,
      backend: backend,
      events: events
    } do
      this = self()
      ref = make_ref()

      ExAws
      |> expect(:request, fn op, opts ->
        send(this, {ref, op, opts})
        {:ok, %{status_code: 200}}
      end)

      assert :ok = S3Adaptor.push_log_events_to_s3({source.id, backend.id}, events)
      assert_received {^ref, op, opts}

      expected_token = source.token |> Atom.to_string() |> String.replace("-", "_")

      assert %ExAws.Operation.S3{http_method: :put, bucket: "my-bucket", path: path, body: body} =
               op

      assert path =~ ~r|^#{expected_token}/\d+\.parquet$|
      assert String.starts_with?(body, "PAR1")
      assert opts[:access_key_id] == "AKID"
      assert opts[:secret_access_key] == "SECRET"
      assert opts[:region] == "us-east-1"
      assert opts[:telemetry_options] == [backend_id: backend.id, bucket: "my-bucket"]
      refute Keyword.has_key?(opts, :scheme)
      refute Keyword.has_key?(opts, :host)
      refute Keyword.has_key?(opts, :port)
    end

    test "passes endpoint overrides parsed from the configured endpoint", %{source: source} do
      backend =
        insert(:backend,
          type: :s3,
          sources: [source],
          config: %{
            s3_bucket: "my-bucket",
            storage_region: "auto",
            access_key_id: "AKID",
            secret_access_key: "SECRET",
            batch_timeout: 1_000,
            endpoint: "https://account-id.r2.cloudflarestorage.com"
          }
        )

      this = self()
      ref = make_ref()

      ExAws
      |> expect(:request, fn op, opts ->
        send(this, {ref, op, opts})
        {:ok, %{status_code: 200}}
      end)

      events = [build(:log_event, source: source)]

      assert :ok = S3Adaptor.push_log_events_to_s3({source.id, backend.id}, events)
      assert_received {^ref, op, opts}
      assert op.bucket == "my-bucket"
      assert opts[:scheme] == "https://"
      assert opts[:host] == "account-id.r2.cloudflarestorage.com"
      assert opts[:port] == 443
    end

    test "folds a path-bearing endpoint into the request bucket", %{source: source} do
      backend =
        insert(:backend,
          type: :s3,
          sources: [source],
          config: %{
            s3_bucket: "my-bucket",
            storage_region: "us-east-1",
            access_key_id: "AKID",
            secret_access_key: "SECRET",
            batch_timeout: 1_000,
            endpoint: "https://project-ref.supabase.co/storage/v1/s3"
          }
        )

      this = self()
      ref = make_ref()

      ExAws
      |> expect(:request, fn op, opts ->
        send(this, {ref, op, opts})
        {:ok, %{status_code: 200}}
      end)

      events = [build(:log_event, source: source)]

      assert :ok = S3Adaptor.push_log_events_to_s3({source.id, backend.id}, events)
      assert_received {^ref, op, opts}
      assert op.bucket == "storage/v1/s3/my-bucket"
      assert opts[:scheme] == "https://"
      assert opts[:host] == "project-ref.supabase.co"
      assert opts[:port] == 443
    end

    test "returns the ExAws error when the upload fails", %{
      source: source,
      backend: backend,
      events: events
    } do
      ExAws
      |> expect(:request, fn _op, _opts ->
        {:error, {:http_error, 403, %{body: "AccessDenied"}}}
      end)

      assert {:error, {:http_error, 403, _body}} =
               S3Adaptor.push_log_events_to_s3({source.id, backend.id}, events)
    end

    test "returns {:error, reason} instead of crashing when parquet serialization panics", %{
      source: source,
      backend: backend,
      events: events
    } do
      Explorer.DataFrame
      |> expect(:dump_parquet, fn _df ->
        raise ErlangError, original: :nif_panicked
      end)

      assert {:error, _reason} =
               S3Adaptor.push_log_events_to_s3({source.id, backend.id}, events)
    end
  end

  describe "handle_request_event/4" do
    test "logs a warning when a request was retried" do
      log =
        capture_log(fn ->
          S3Adaptor.handle_request_event(
            [:logflare, :backends, :s3, :request, :stop],
            %{},
            %{
              attempt: 2,
              options: [backend_id: 123, bucket: "my-bucket"],
              result: :error,
              error: "timeout"
            },
            nil
          )
        end)

      assert log =~ "S3 adaptor request retry: attempt 2"
    end

    test "does not log on the first attempt" do
      log =
        capture_log(fn ->
          S3Adaptor.handle_request_event(
            [:logflare, :backends, :s3, :request, :stop],
            %{},
            %{attempt: 1, options: [backend_id: 123, bucket: "my-bucket"], result: :ok},
            nil
          )
        end)

      assert log == ""
    end
  end
end
