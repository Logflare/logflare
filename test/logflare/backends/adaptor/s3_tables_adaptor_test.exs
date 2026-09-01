defmodule Logflare.Backends.Adaptor.S3TablesAdaptorTest do
  use Logflare.DataCase, async: true

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema

  doctest S3TablesAdaptor

  @valid_config %{
    table_bucket_arn: "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
    access_key_id: "aws_key_id",
    secret_access_key: "aws_secret_key",
    namespace: "my_namespace",
    batch_timeout: 5_000
  }

  test "config_validation" do
    assert %Ecto.Changeset{valid?: true} =
             Adaptor.cast_and_validate_config(S3TablesAdaptor, @valid_config)

    configs =
      [
        Map.delete(@valid_config, :table_bucket_arn),
        Map.delete(@valid_config, :access_key_id),
        Map.delete(@valid_config, :secret_access_key),
        Map.delete(@valid_config, :namespace),
        %{@valid_config | batch_timeout: 999},
        %{@valid_config | batch_timeout: 66_000}
      ]

    for config <- configs do
      assert %Ecto.Changeset{valid?: false} =
               Adaptor.cast_and_validate_config(S3TablesAdaptor, config)
    end
  end

  test "redact_config/1" do
    config = %{secret_access_key: "secret-key-123", table_bucket_arn: "arn:aws:..."}
    assert %{secret_access_key: "REDACTED"} = S3TablesAdaptor.redact_config(config)
  end

  describe "Native module (integration)" do
    @describetag :integration
    test "invalid credentials" do
      assert {:error, err} = S3TablesAdaptor.Native.init_catalog(@valid_config)
      assert err =~ "invalid"
    end

    setup do
      table_bucket_arn = System.fetch_env!("LOGFLARE_S3_TABLES_TEST_BUCKET_ARN")
      namespace = System.fetch_env!("LOGFLARE_S3_TABLES_TEST_NAMESPACE")
      access_key_id = System.fetch_env!("AWS_ACCESS_KEY_ID")
      secret_access_key = System.fetch_env!("AWS_SECRET_ACCESS_KEY")

      config =
        %{
          table_bucket_arn: table_bucket_arn,
          namespace: namespace,
          access_key_id: access_key_id,
          secret_access_key: secret_access_key
        }

      # drop the OTEL tables before an integration run so tables created by
      # earlier schema revisions don't leak their stale schemas into the tests
      {:ok, catalog} = S3TablesAdaptor.Native.init_catalog(config)

      for event_type <- IcebergSchema.event_types() do
        S3TablesAdaptor.Native.drop_table(catalog, IcebergSchema.table_name(event_type))
      end

      %{config: config}
    end

    test "ensure_table/4 and table_info/2", %{config: config} do
      assert {:ok, catalog} = S3TablesAdaptor.Native.init_catalog(config)

      for event_type <- IcebergSchema.event_types() do
        table_name = IcebergSchema.table_name(event_type)
        fields = IcebergSchema.fields(event_type)
        properties = IcebergSchema.table_properties(event_type)

        assert {:ok, _status} =
                 S3TablesAdaptor.Native.ensure_table(catalog, table_name, fields, properties)

        assert {:ok, :already_exists} =
                 S3TablesAdaptor.Native.ensure_table(catalog, table_name, fields, properties)

        assert {:ok, info} = S3TablesAdaptor.Native.table_info(catalog, table_name)
        assert info.columns == Enum.map(fields, & &1.name)

        assert info.properties["logflare.schema-version"] ==
                 IcebergSchema.schema_version(event_type)
      end
    end

    test "append_batch/3 snapshot generation", %{config: config} do
      assert {:ok, catalog} = S3TablesAdaptor.Native.init_catalog(config)
      table_name = IcebergSchema.table_name(:log)

      assert {:ok, _status} =
               S3TablesAdaptor.Native.ensure_table(
                 catalog,
                 table_name,
                 IcebergSchema.fields(:log),
                 IcebergSchema.table_properties(:log)
               )

      {:ok, snapshot_before} = S3TablesAdaptor.Native.snapshot_info(catalog, table_name)
      snapshots_before = if snapshot_before, do: snapshot_before.snapshot_count, else: 0

      now_us = System.os_time(:microsecond)

      ndjson =
        for n <- 1..3, into: "" do
          row = %{
            "id" => Ecto.UUID.generate(),
            "event_message" => "integration test event #{n}",
            "timestamp" => now_us,
            "log_attributes" => %{"n" => "#{n}"}
          }

          Jason.encode!(row) <> "\n"
        end

      assert {:ok, %{row_count: 3, data_files: data_files}} =
               S3TablesAdaptor.Native.append_batch(catalog, table_name, ndjson)

      assert data_files >= 1

      assert {:ok, snapshot} = S3TablesAdaptor.Native.snapshot_info(catalog, table_name)
      assert snapshot.snapshot_count == snapshots_before + 1
      assert snapshot.operation == "append"
      assert snapshot.summary["added-records"] == "3"
    end

    test "concurrent appends", %{config: config} do
      assert {:ok, catalog} = S3TablesAdaptor.Native.init_catalog(config)
      table_name = IcebergSchema.table_name(:log)

      assert {:ok, _status} =
               S3TablesAdaptor.Native.ensure_table(
                 catalog,
                 table_name,
                 IcebergSchema.fields(:log),
                 IcebergSchema.table_properties(:log)
               )

      now_us = System.os_time(:microsecond)

      results =
        1..2
        |> Task.async_stream(
          fn n ->
            row = %{
              "id" => Ecto.UUID.generate(),
              "event_message" => "concurrent append #{n}",
              "timestamp" => now_us
            }

            S3TablesAdaptor.Native.append_batch(catalog, table_name, Jason.encode!(row) <> "\n")
          end,
          timeout: 120_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert [{:ok, %{row_count: 1}}, {:ok, %{row_count: 1}}] = results
    end
  end
end
