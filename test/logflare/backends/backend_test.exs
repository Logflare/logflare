defmodule Logflare.Backends.BackendTest do
  use Logflare.DataCase
  alias Logflare.Backends.Backend

  describe "changeset/2" do
    setup do
      user = insert(:user)
      {:ok, user: user}
    end

    test "validates `default_ingest?` for BigQuery backend", %{user: user} do
      attrs = %{
        name: "Test BigQuery Backend",
        type: :bigquery,
        config: %{project_id: "test-project", dataset_id: "test-dataset"},
        user_id: user.id,
        default_ingest?: true
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      assert changeset.valid?
      assert get_change(changeset, :default_ingest?) == true
    end

    test "validates `default_ingest?` for ClickHouse backend", %{user: user} do
      attrs = %{
        name: "Test ClickHouse Backend",
        type: :clickhouse,
        config: %{url: "http://localhost:8123", database: "default", port: 8123},
        user_id: user.id,
        default_ingest?: true
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      assert changeset.valid?
      assert get_change(changeset, :default_ingest?) == true
    end

    test "validates `default_ingest?` for Postgres backend", %{user: user} do
      attrs = %{
        name: "Test Postgres Backend",
        type: :postgres,
        config: %{url: "postgres://localhost/test"},
        user_id: user.id,
        default_ingest?: true
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      assert changeset.valid?
      assert get_change(changeset, :default_ingest?) == true
    end

    test "validates `default_ingest?` for S3 backend", %{user: user} do
      attrs = %{
        name: "Test S3 Backend",
        type: :s3,
        config: %{
          access_key_id: "test-key",
          secret_access_key: "test-secret",
          s3_bucket: "test-bucket",
          storage_region: "us-east-1"
        },
        user_id: user.id,
        default_ingest?: true
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      assert changeset.valid?
      assert get_change(changeset, :default_ingest?) == true
    end

    test "rejects `default_ingest?` for webhook backend", %{user: user} do
      attrs = %{
        name: "Test Webhook Backend",
        type: :webhook,
        config: %{url: "https://example.com/webhook"},
        user_id: user.id,
        default_ingest?: true
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      refute changeset.valid?

      assert errors_on(changeset)[:default_ingest?] == [
               "Backend type webhook does not support default ingest"
             ]
    end

    test "rejects `default_ingest?` for datadog backend", %{user: user} do
      attrs = %{
        name: "Test Datadog Backend",
        type: :datadog,
        config: %{api_key: "test-api-key", url: "https://api.datadoghq.com"},
        user_id: user.id,
        default_ingest?: true
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      refute changeset.valid?

      assert errors_on(changeset)[:default_ingest?] == [
               "Backend type datadog does not support default ingest"
             ]
    end

    test "allows `default_ingest?` false for any backend type", %{user: user} do
      attrs = %{
        name: "Test Webhook Backend",
        type: :webhook,
        config: %{url: "https://example.com/webhook"},
        user_id: user.id,
        default_ingest?: false
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      assert changeset.valid?
      assert get_field(changeset, :default_ingest?) == false
    end

    test "defaults `default_ingest?` to false when not provided", %{user: user} do
      attrs = %{
        name: "Test BigQuery Backend",
        type: :bigquery,
        config: %{project_id: "test-project", dataset_id: "test-dataset"},
        user_id: user.id
      }

      changeset = Backend.changeset(%Backend{}, attrs)
      assert changeset.valid?
      assert get_change(changeset, :default_ingest?) == nil
      assert get_field(changeset, :default_ingest?) == false
    end

    test "validates `default_ingest?` only when changing to true", %{user: user} do
      backend =
        insert(:backend, type: :webhook, user: user, config: %{url: "https://example.com"})

      attrs = %{name: "Updated Name"}
      changeset = Backend.changeset(backend, attrs)
      assert changeset.valid?

      attrs = %{default_ingest?: true}
      changeset = Backend.changeset(backend, attrs)
      refute changeset.valid?

      assert errors_on(changeset)[:default_ingest?] == [
               "Backend type webhook does not support default ingest"
             ]
    end
  end
end
