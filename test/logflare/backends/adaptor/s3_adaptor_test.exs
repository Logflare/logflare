defmodule Logflare.Backends.Adaptor.S3AdaptorTest do
  use Logflare.DataCase, async: true

  alias Explorer.DataFrame
  alias Logflare.Backends.Adaptor.S3Adaptor

  doctest S3Adaptor

  describe "redact_config/1" do
    test "redacts secret_access_key when present" do
      config = %{secret_access_key: "secret-key-123", bucket_name: "my-bucket"}
      assert %{secret_access_key: "REDACTED"} = S3Adaptor.redact_config(config)
    end
  end

  describe "push_log_events_to_s3/2" do
    test "emits egress telemetry for every batch sent" do
      start_supervised!(Logflare.SystemMetrics.AllLogsLogged)
      insert(:plan)

      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :s3,
          sources: [source],
          config: %{
            s3_bucket: "test-bucket",
            storage_region: "us-east-1",
            access_key_id: "test-key",
            secret_access_key: "test-secret"
          },
          user: user,
          metadata: %{"environment" => "test", "region" => "us-west"}
        )

      stub(DataFrame, :to_parquet, fn _df, _path, _opts -> :ok end)

      test_ref = make_ref()
      pid = self()

      :telemetry.attach(
        {__MODULE__, test_ref},
        [:logflare, :backends, :ingest, :egress],
        fn _event, measurements, metadata, _config ->
          send(pid, {test_ref, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach({__MODULE__, test_ref}) end)

      log_event = build(:log_event, source: source, message: "s3 egress check")

      assert :ok = S3Adaptor.push_log_events_to_s3({source.id, backend.id}, [log_event])

      assert_receive {^test_ref, %{request_bytes: request_bytes}, metadata}, 5_000
      assert request_bytes > 0

      assert %{
               "source_id" => source.id,
               "source_uuid" => source.token,
               "backend_id" => backend.id,
               "backend_uuid" => backend.token,
               "user_id" => source.user_id,
               "backend.environment" => "test",
               "backend.region" => "us-west"
             } == metadata
    end
  end
end
