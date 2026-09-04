defmodule Logflare.Backends.Adaptor.S3TablesAdaptorTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.CatalogManager
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Native
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Pipeline
  alias Logflare.Mapper.OtelDefaults
  alias Logflare.SystemMetrics.AllLogsLogged

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

  describe "ingestion through the adaptor supervision tree" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :s3_tables,
          sources: [source],
          user: user,
          config: Map.put(@valid_config, :batch_timeout, 100)
        )

      catalog = make_ref()
      Mimic.stub(Native, :init_catalog, fn _config -> {:ok, catalog} end)

      Mimic.stub(Native, :ensure_table, fn _catalog, _table, _fields, _props ->
        {:ok, :created}
      end)

      start_supervised!(AllLogsLogged)
      start_supervised!({S3TablesAdaptor, backend})

      [source: source, backend: backend, catalog: catalog]
    end

    test "log event", %{source: source, backend: backend, catalog: catalog} do
      start_supervised!({CatalogManager, backend})
      test_pid = self()
      backend_id = backend.id

      Mimic.expect(Native, :append_batch, fn ^catalog, table_name, ndjson ->
        send(test_pid, {:appended, table_name, ndjson})
        {:ok, %{row_count: 1, data_files: 1}}
      end)

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :pipeline, :handle_batch],
        [:logflare, :backends, :s3_tables, :append]
      ])

      assert {:ok, _} =
               Backends.ingest_logs(
                 [%{"event_message" => "adaptor level test", "metadata" => %{"level" => "info"}}],
                 source
               )

      assert_receive {:appended, "otel_logs", ndjson}, 5_000

      assert [row] = decode_ndjson(ndjson)
      assert row["event_message"] == "adaptor level test"
      assert row["mapping_config_id"] == OtelDefaults.config_id(:log)
      assert row["source_uuid"] == to_string(source.token)
      assert is_binary(row["id"])
      assert is_integer(row["ingested_at"])
      assert is_map(row["log_attributes"])

      assert_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, %{batch_size: 1},
                      %{
                        backend_type: :s3_tables,
                        backend_id: ^backend_id,
                        event_type: :log,
                        day_bucket: _
                      }},
                     5_000

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref,
                      %{duration_us: _, row_count: 1, data_files: 1},
                      %{status: :ok, backend_id: ^backend_id, event_type: :log}},
                     5_000
    end

    test "metric and trace events", %{source: source, backend: backend} do
      start_supervised!({CatalogManager, backend})
      test_pid = self()

      Mimic.expect(Native, :append_batch, 2, fn _catalog, table_name, ndjson ->
        send(test_pid, {:appended, table_name, ndjson})
        {:ok, %{row_count: 1, data_files: 1}}
      end)

      for {type, table_name} <- [{"metric", "otel_metrics"}, {"span", "otel_traces"}] do
        assert {:ok, _} =
                 Backends.ingest_logs(
                   [%{"event_message" => "typed event", "metadata" => %{"type" => type}}],
                   source
                 )

        assert_receive {:appended, ^table_name, ndjson}, 5_000
        assert [%{"event_message" => "typed event"}] = decode_ndjson(ndjson)
      end
    end

    test "append failure", %{source: source, backend: backend} do
      start_supervised!({CatalogManager, backend})
      test_pid = self()
      backend_id = backend.id
      attempts = Pipeline.max_retries() + 1

      Mimic.expect(Native, :append_batch, attempts, fn _catalog, _table_name, _ndjson ->
        send(test_pid, :append_attempt)
        {:error, :commit_conflict}
      end)

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :s3_tables, :append]
      ])

      log =
        capture_log(fn ->
          assert {:ok, _} = Backends.ingest_logs([%{"event_message" => "doomed"}], source)

          for _ <- 1..attempts, do: assert_receive(:append_attempt, 5_000)
          refute_receive :append_attempt, 1_000
        end)

      assert log =~ "S3 Tables append failed"
      assert log =~ "exhausted #{Pipeline.max_retries()} retries"

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref, %{duration_us: _},
                      %{status: :error, reason: :commit_conflict, backend_id: ^backend_id}},
                     5_000
    end

    test "catalog not provisioned", %{source: source, backend: backend} do
      Mimic.reject(&Native.append_batch/3)
      backend_id = backend.id

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :pipeline, :handle_batch]
      ])

      log =
        capture_log(fn ->
          assert {:ok, _} = Backends.ingest_logs([%{"event_message" => "no catalog"}], source)

          # the failed batch is retried once, then dropped
          assert_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, _,
                          %{backend_type: :s3_tables, backend_id: ^backend_id}},
                         5_000

          assert_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, _,
                          %{backend_type: :s3_tables, backend_id: ^backend_id}},
                         5_000

          refute_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref, _,
                          %{backend_type: :s3_tables, backend_id: ^backend_id}},
                         1_000
        end)

      assert log =~ "S3 Tables append failed"
    end
  end

  defp decode_ndjson(ndjson) do
    ndjson
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
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
