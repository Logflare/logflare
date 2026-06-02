defmodule Logflare.Backends.Adaptor.S3AdaptorTest do
  use Logflare.DataCase, async: true

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

  describe "validate_config/1 SSRF protection" do
    test "rejects endpoints targeting private/reserved IP addresses" do
      blocked = [
        # loopback
        "http://127.0.0.1/",
        # RFC1918
        "http://10.0.0.1/",
        "http://172.16.0.1/",
        "http://192.168.1.1/",
        # link-local / cloud metadata
        "http://169.254.169.254/latest/meta-data/",
        # all-zeros, CGNAT
        "http://0.0.0.0/",
        "http://100.64.0.1/",
        # private IPv6
        "http://[::1]/",
        "http://[fc00::1]/",
        "http://[fd00::1]/"
      ]

      for endpoint <- blocked do
        cs =
          Adaptor.cast_and_validate_config(S3Adaptor, Map.put(@valid_config, :endpoint, endpoint))

        assert {_message, [validation: :ssrf]} = cs.errors[:endpoint],
               "expected SSRF block for #{endpoint}"
      end
    end

    test "allows public endpoint addresses (172.16.0.0/12 boundary)" do
      for endpoint <- ["http://172.15.0.1/", "http://172.32.0.1/"] do
        assert %Ecto.Changeset{valid?: true} =
                 Adaptor.cast_and_validate_config(
                   S3Adaptor,
                   Map.put(@valid_config, :endpoint, endpoint)
                 ),
               "expected valid for #{endpoint}"
      end
    end

    test "rejects endpoint hostname resolving to loopback" do
      cs =
        Adaptor.cast_and_validate_config(
          S3Adaptor,
          Map.put(@valid_config, :endpoint, "http://localhost/")
        )

      assert %Ecto.Changeset{valid?: false} = cs
      assert {_message, [validation: :ssrf]} = cs.errors[:endpoint]
    end

    test "allows a config with no endpoint (default AWS S3)" do
      assert %Ecto.Changeset{valid?: true} =
               Adaptor.cast_and_validate_config(S3Adaptor, @valid_config)
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
