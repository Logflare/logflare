defmodule Logflare.Backends.Adaptor.BigQueryV2AdaptorTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias GoogleApi.BigQuery.V2.Api.Datasets
  alias GoogleApi.BigQuery.V2.Api.Tables
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor
  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor.Provisioner
  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor.TableSchema
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates
  alias Logflare.Backends.Backend

  @config %{project_id: "my-project-id", dataset_id: "my_dataset"}

  setup do
    user = insert(:user)
    backend = insert(:backend, type: :bigquery_v2, config: @config, user: user)
    [backend: %{backend | config: @config, user: user}]
  end

  describe "adaptor registration" do
    test "maps the bigquery_v2 type to the adaptor", %{backend: backend} do
      assert Adaptor.get_adaptor(backend) == BigQueryV2Adaptor
    end

    test "uses consolidated ingest and is not a default ingest backend", %{backend: backend} do
      assert Adaptor.consolidated_ingest?(backend)
      refute Adaptor.supports_default_ingest?(backend)
    end
  end

  describe "validate_config/1" do
    test "accepts a valid project_id and dataset_id" do
      assert Adaptor.cast_and_validate_config(BigQueryV2Adaptor, @config).valid?
    end

    test "requires project_id and dataset_id" do
      changeset = Adaptor.cast_and_validate_config(BigQueryV2Adaptor, %{})

      refute changeset.valid?
      assert Keyword.has_key?(changeset.errors, :project_id)
      assert Keyword.has_key?(changeset.errors, :dataset_id)
    end

    test "rejects identifiers BigQuery would not accept" do
      changeset =
        Adaptor.cast_and_validate_config(BigQueryV2Adaptor, %{
          project_id: "evil;drop",
          dataset_id: "my-dataset"
        })

      refute changeset.valid?
      assert Keyword.has_key?(changeset.errors, :project_id)
      assert Keyword.has_key?(changeset.errors, :dataset_id)
    end
  end

  describe "TableSchema.to_table/3" do
    test "builds a day-partitioned, clustered otel_logs table" do
      table = TableSchema.to_table(:log, "my-project-id", "my_dataset")

      assert %Model.TableReference{
               projectId: "my-project-id",
               datasetId: "my_dataset",
               tableId: "otel_logs"
             } = table.tableReference

      assert %Model.TimePartitioning{field: "timestamp", type: "DAY"} = table.timePartitioning
      assert table.clustering.fields == ["project", "source_uuid", "timestamp"]
      assert table.labels["logflare_schema_version"] == TableSchema.schema_version(:log)
    end

    test "marks tenancy columns as required and the rest as nullable" do
      fields = Map.new(TableSchema.to_table(:log, "p", "d").schema.fields, &{&1.name, &1})

      for name <- ["id", "project", "source_uuid", "timestamp"] do
        assert fields[name].mode == "REQUIRED"
      end

      assert %{type: "STRING", mode: "NULLABLE"} = fields["event_message"]
      assert %{type: "INT64", mode: "NULLABLE"} = fields["severity_number"]
      assert %{type: "TIMESTAMP", mode: "REQUIRED"} = fields["timestamp"]
    end

    test "stores attribute maps as repeated key/value records" do
      fields = Map.new(TableSchema.to_table(:log, "p", "d").schema.fields, &{&1.name, &1})

      for name <- ["resource_attributes", "scope_attributes", "log_attributes"] do
        assert %{type: "RECORD", mode: "REPEATED", fields: [key, value]} = fields[name]
        assert %{name: "key", type: "STRING", mode: "REQUIRED"} = key
        assert %{name: "value", type: "STRING", mode: "NULLABLE"} = value
      end
    end

    test "has the same columns as the ClickHouse tables, with `.` replaced by `_`" do
      for event_type <- [:log, :metric, :trace] do
        columns = Enum.map(TableSchema.fields(event_type), & &1.name)
        clickhouse_columns = QueryTemplates.columns_for_type(event_type)

        assert columns == Enum.map(clickhouse_columns, &String.replace(&1, ".", "_"))
      end
    end

    test "builds otel_metrics and otel_traces tables with the same tenancy layout" do
      for {event_type, table_id} <- [metric: "otel_metrics", trace: "otel_traces"] do
        table = TableSchema.to_table(event_type, "p", "d")
        fields = Map.new(table.schema.fields, &{&1.name, &1})

        assert table.tableReference.tableId == table_id
        assert table.clustering.fields == ["project", "source_uuid", "timestamp"]
        assert table.labels["logflare_schema_version"] == TableSchema.schema_version(event_type)

        for name <- ["id", "project", "source_uuid", "timestamp"] do
          assert fields[name].mode == "REQUIRED"
        end
      end
    end

    test "maps metric value columns to typed BigQuery columns" do
      fields = Map.new(TableSchema.to_table(:metric, "p", "d").schema.fields, &{&1.name, &1})

      assert %{type: "FLOAT64", mode: "NULLABLE"} = fields["sum"]
      assert %{type: "BOOL", mode: "NULLABLE"} = fields["is_monotonic"]
      assert %{type: "INT64", mode: "REPEATED"} = fields["bucket_counts"]
      assert %{type: "FLOAT64", mode: "REPEATED"} = fields["explicit_bounds"]
      assert %{type: "INT64", mode: "REPEATED"} = fields["exemplars_time_unix"]
    end

    test "keeps nested lists at one level, with per-element attributes as JSON strings" do
      metric_fields =
        Map.new(TableSchema.to_table(:metric, "p", "d").schema.fields, &{&1.name, &1})

      trace_fields = Map.new(TableSchema.to_table(:trace, "p", "d").schema.fields, &{&1.name, &1})

      assert %{type: "STRING", mode: "REPEATED", fields: nil} =
               metric_fields["exemplars_filtered_attributes"]

      for name <- ["events_attributes", "links_attributes"] do
        assert %{type: "STRING", mode: "REPEATED", fields: nil} = trace_fields[name]
      end

      assert %{type: "STRING", mode: "REPEATED"} = trace_fields["events_name"]
      assert %{type: "INT64", mode: "REPEATED"} = trace_fields["events_timestamp"]
    end
  end

  describe "TableSchema.schema_version/1" do
    test "fits in a BigQuery label value" do
      for event_type <- [:log, :metric, :trace] do
        version = TableSchema.schema_version(event_type)

        assert String.length(version) == 32
        assert version =~ ~r/\A[0-9a-f]+\z/
      end
    end

    test "differs between tables" do
      versions = Enum.map([:log, :metric, :trace], &TableSchema.schema_version/1)
      assert Enum.uniq(versions) == versions
    end
  end

  describe "test_connection/1" do
    test "returns :ok when the dataset exists", %{backend: backend} do
      expect(Datasets, :bigquery_datasets_get, fn _conn, "my-project-id", "my_dataset" ->
        {:ok, %Model.Dataset{}}
      end)

      assert :ok = BigQueryV2Adaptor.test_connection(backend)
    end

    test "returns an error when the dataset is missing", %{backend: backend} do
      stub(Datasets, :bigquery_datasets_get, fn _conn, _project_id, _dataset_id ->
        {:error, google_error(404, "Not found: Dataset my-project-id:my_dataset")}
      end)

      assert {:error, :dataset_unavailable} = BigQueryV2Adaptor.test_connection(backend)
    end
  end

  describe "check_dataset/1" do
    test "returns :ok when the dataset exists", %{backend: backend} do
      expect(Datasets, :bigquery_datasets_get, fn _conn, "my-project-id", "my_dataset" ->
        {:ok, %Model.Dataset{}}
      end)

      assert :ok = BigQueryV2Adaptor.check_dataset(backend)
    end

    test "returns the Google API error message when the dataset is missing", %{backend: backend} do
      stub(Datasets, :bigquery_datasets_get, fn _conn, _project_id, _dataset_id ->
        {:error, google_error(404, "Not found: Dataset my-project-id:my_dataset")}
      end)

      assert {:error, "Not found: Dataset my-project-id:my_dataset"} =
               BigQueryV2Adaptor.check_dataset(backend)
    end
  end

  describe "provision_tables/1" do
    test "creates the otel_logs, otel_metrics and otel_traces tables", %{backend: backend} do
      pid = self()

      expect(Tables, :bigquery_tables_insert, 3, fn _conn, "my-project-id", "my_dataset", opts ->
        send(pid, {:inserted, opts[:body].tableReference.tableId})
        {:ok, opts[:body]}
      end)

      reject(Tables, :bigquery_tables_get, 4)

      assert :ok = BigQueryV2Adaptor.provision_tables(backend)
      assert_received {:inserted, "otel_logs"}
      assert_received {:inserted, "otel_metrics"}
      assert_received {:inserted, "otel_traces"}
    end

    test "leaves an existing table with a matching schema version alone", %{backend: backend} do
      stub(Tables, :bigquery_tables_insert, fn _conn, _project_id, _dataset_id, _opts ->
        {:error, google_error(409, "Already Exists")}
      end)

      tables = Map.new(TableSchema.event_types(), &{TableSchema.table_name(&1), &1})

      expect(Tables, :bigquery_tables_get, 3, fn _conn, "my-project-id", "my_dataset", table_id ->
        {:ok, TableSchema.to_table(Map.fetch!(tables, table_id), "my-project-id", "my_dataset")}
      end)

      log = capture_log(fn -> assert :ok = BigQueryV2Adaptor.provision_tables(backend) end)
      refute log =~ "schema mismatch"
    end

    test "warns on schema drift without failing", %{backend: backend} do
      stub(Tables, :bigquery_tables_insert, fn _conn, _project_id, _dataset_id, _opts ->
        {:error, google_error(409, "Already Exists")}
      end)

      stub(Tables, :bigquery_tables_get, fn _conn, _project_id, _dataset_id, _table ->
        table = TableSchema.to_table(:log, "my-project-id", "my_dataset")
        fields = Enum.reject(table.schema.fields, &(&1.name == "trace_flags"))

        {:ok,
         %{
           table
           | labels: %{"logflare_schema_version" => "outdated"},
             schema: %Model.TableSchema{fields: fields}
         }}
      end)

      log = capture_log(fn -> assert :ok = BigQueryV2Adaptor.provision_tables(backend) end)
      assert log =~ "BigQuery v2 table schema mismatch"
    end

    test "returns the Google API error message when creation fails", %{backend: backend} do
      stub(Tables, :bigquery_tables_insert, fn _conn, _project_id, _dataset_id, _opts ->
        {:error, google_error(403, "Access Denied")}
      end)

      assert {:error, "Access Denied"} = BigQueryV2Adaptor.provision_tables(backend)
    end
  end

  describe "Provisioner" do
    test "is a transient child", %{backend: backend} do
      assert %{restart: :transient, start: {Provisioner, :start_link, [^backend]}} =
               Provisioner.child_spec(backend)
    end

    test "provisions tables and exits normally", %{backend: backend} do
      stub(Datasets, :bigquery_datasets_get, fn _conn, _project_id, _dataset_id ->
        {:ok, %Model.Dataset{}}
      end)

      stub(Tables, :bigquery_tables_insert, fn _conn, _project_id, _dataset_id, opts ->
        {:ok, opts[:body]}
      end)

      {:ok, pid} = Provisioner.start_link(backend)
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
    end

    @tag capture_log: true
    test "stops without provisioning when the dataset is unreachable", %{backend: backend} do
      stub(Datasets, :bigquery_datasets_get, fn _conn, _project_id, _dataset_id ->
        {:error, google_error(404, "Not found")}
      end)

      reject(Tables, :bigquery_tables_insert, 4)

      Process.flag(:trap_exit, true)
      {:ok, pid} = Provisioner.start_link(backend)

      assert_receive {:EXIT, ^pid, {:shutdown, {:error, "Not found"}}}, 5_000
    end
  end

  describe "start_link/1" do
    test "starts the adaptor supervisor", %{backend: %Backend{} = backend} do
      pid = start_supervised!({BigQueryV2Adaptor, backend})
      assert Process.alive?(pid)
    end
  end

  defp google_error(status, message) do
    %Tesla.Env{status: status, body: Jason.encode!(%{"error" => %{"message" => message}})}
  end
end
