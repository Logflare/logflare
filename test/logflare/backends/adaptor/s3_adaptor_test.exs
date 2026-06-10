defmodule Logflare.Backends.Adaptor.S3AdaptorTest do
  use Logflare.DataCase, async: false

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

    test "writes a sentinel parquet to the fixed probe key", %{backend: backend} do
      this = self()
      ref = make_ref()

      Explorer.DataFrame
      |> expect(:to_parquet, fn _df, path, opts ->
        send(this, {ref, path, opts[:config]})
        :ok
      end)

      assert :ok = S3Adaptor.test_connection(backend)
      assert_received {^ref, "s3://my-bucket/_connection_test.parquet", config}
      assert config[:access_key_id] == "AKID"
      assert config[:secret_access_key] == "SECRET"
      assert config[:region] == "us-east-1"
    end

    test "returns error when the write fails", %{backend: backend} do
      Explorer.DataFrame
      |> expect(:to_parquet, fn _df, _path, _opts ->
        {:error, %RuntimeError{message: "Generic S3 error: AccessDenied"}}
      end)

      assert {:error, reason} = S3Adaptor.test_connection(backend)
      assert reason =~ "AccessDenied"
    end
  end
end
