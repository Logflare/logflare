defmodule Logflare.Backends.Adaptor.S3TablesAdaptorTest do
  use Logflare.DataCase, async: true

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor

  doctest S3TablesAdaptor

  @valid_config %{
    table_bucket_arn: "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
    access_key_id: "aws_key_id",
    secret_access_key: "aws_secret_key",
    namespace: "my_namespace",
    batch_timeout: 1_000
  }

  describe "cast_config/2 and validate_config/1" do
    test "valid config" do
      assert %Ecto.Changeset{valid?: true} =
               Adaptor.cast_and_validate_config(S3TablesAdaptor, @valid_config)
    end

    test "missing property" do
      for key <- Map.keys(@valid_config), key != :batch_timeout do
        cs =
          Adaptor.cast_and_validate_config(
            S3TablesAdaptor,
            Map.delete(@valid_config, key)
          )

        assert %Ecto.Changeset{valid?: false} = cs
        assert {_message, [validation: :required]} = cs.errors[key]
      end
    end

    test "batch_timeout below the minimum" do
      cs =
        Adaptor.cast_and_validate_config(
          S3TablesAdaptor,
          Map.put(@valid_config, :batch_timeout, 999)
        )

      assert %Ecto.Changeset{valid?: false} = cs

      assert {_message, [validation: :number, kind: :greater_than_or_equal_to, number: 1_000]} =
               cs.errors[:batch_timeout]
    end

    test "batch_timeout above the maximum" do
      cs =
        Adaptor.cast_and_validate_config(
          S3TablesAdaptor,
          Map.put(@valid_config, :batch_timeout, 5_001)
        )

      assert %Ecto.Changeset{valid?: false} = cs

      assert {_message, [validation: :number, kind: :less_than_or_equal_to, number: 5_000]} =
               cs.errors[:batch_timeout]
    end
  end

  test "redact_config/1" do
    config = %{secret_access_key: "secret-key-123", table_bucket_arn: "arn:aws:..."}
    assert %{secret_access_key: "REDACTED"} = S3TablesAdaptor.redact_config(config)
  end

  describe "Native module" do
    test "invalid credentials" do
      assert {:error, err} = S3TablesAdaptor.Native.init_catalog(@valid_config)
      assert err =~ "invalid"
    end
  end
end
