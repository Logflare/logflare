defmodule Logflare.Backends.Adaptor.S3AdaptorTest do
  use Logflare.DataCase, async: true

  alias Logflare.Backends.Adaptor.S3Adaptor

  doctest S3Adaptor

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
