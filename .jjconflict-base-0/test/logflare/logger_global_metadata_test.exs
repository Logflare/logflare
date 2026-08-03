defmodule Logflare.LoggerGlobalMetadataTest do
  use ExUnit.Case, async: false

  require Logger

  alias Logflare.Application, as: App

  defmodule CaptureHandler do
    def log(event, %{config: %{pid: pid}}), do: send(pid, {:log_event, event})
  end

  setup do
    original_primary = :logger.get_primary_config()
    original_app_meta = Application.get_env(:logflare, :metadata)

    :ok =
      :logger.add_handler(:global_meta_capture, __MODULE__.CaptureHandler, %{
        level: :all,
        config: %{pid: self()}
      })

    on_exit(fn ->
      :logger.remove_handler(:global_meta_capture)
      :logger.set_primary_config(original_primary)

      if original_app_meta,
        do: Application.put_env(:logflare, :metadata, original_app_meta),
        else: Application.delete_env(:logflare, :metadata)
    end)

    :ok
  end

  test "log events carry the Logflare version in metadata" do
    expected = Application.spec(:logflare, :vsn) |> to_string()
    assert expected =~ ~r/\d+\.\d+\.\d+/

    Logger.info("logflare_version metadata probe")

    assert_receive {:log_event, %{meta: %{logflare_version: ^expected}}}
  end

  test "log events carry the cluster in metadata when configured" do
    Application.put_env(:logflare, :metadata, cluster: "test-cluster")
    :logger.update_primary_config(%{metadata: App.global_logger_metadata()})

    Logger.info("cluster metadata probe")

    assert_receive {:log_event, %{meta: %{cluster: "test-cluster"}}}
  end

  test "global_logger_metadata/0 merges the version with configured :logflare metadata" do
    Application.put_env(:logflare, :metadata, cluster: "abc")

    meta = App.global_logger_metadata()

    assert meta.cluster == "abc"
    assert meta.logflare_version == Application.spec(:logflare, :vsn) |> to_string()
  end
end
