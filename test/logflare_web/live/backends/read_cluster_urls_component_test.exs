defmodule LogflareWeb.Backends.ReadClusterUrlsComponentTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Backend
  alias LogflareWeb.Backends.ReadClusterUrlsComponent
  alias Phoenix.LiveView.Socket

  describe "assemble_read_only_urls/1" do
    test "assembles flat form params into the read_only_urls map" do
      config = %{
        "url" => "http://ingest.local:8123",
        "read_cluster_label_0" => "reporting",
        "read_cluster_url_0" => "http://reporting.local:8123",
        "read_cluster_label_1" => "adhoc",
        "read_cluster_url_1" => "http://adhoc.local:8123"
      }

      assert {:ok, assembled} = ReadClusterUrlsComponent.assemble_read_only_urls(config)

      assert assembled["read_only_urls"] == %{
               "reporting" => "http://reporting.local:8123",
               "adhoc" => "http://adhoc.local:8123"
             }

      assert assembled["url"] == "http://ingest.local:8123"
      refute Map.has_key?(assembled, "read_cluster_label_0")
      refute Map.has_key?(assembled, "read_cluster_url_0")
    end

    test "allows the same URL under different labels" do
      config = %{
        "read_cluster_label_0" => "reporting",
        "read_cluster_url_0" => "http://shared.local:8123",
        "read_cluster_label_1" => "adhoc",
        "read_cluster_url_1" => "http://shared.local:8123"
      }

      assert {:ok, assembled} = ReadClusterUrlsComponent.assemble_read_only_urls(config)

      assert assembled["read_only_urls"] == %{
               "reporting" => "http://shared.local:8123",
               "adhoc" => "http://shared.local:8123"
             }
    end

    test "rejects duplicate labels" do
      config = %{
        "read_cluster_label_0" => "same",
        "read_cluster_url_0" => "http://a.local:8123",
        "read_cluster_label_1" => "same",
        "read_cluster_url_1" => "http://b.local:8123"
      }

      assert {:error, message} = ReadClusterUrlsComponent.assemble_read_only_urls(config)
      assert message =~ "Duplicate read cluster labels"
      assert message =~ "same"
    end

    test "skips rows with a blank label or blank URL" do
      config = %{
        "read_cluster_label_0" => "reporting",
        "read_cluster_url_0" => "http://reporting.local:8123",
        "read_cluster_label_1" => "",
        "read_cluster_url_1" => "http://orphan-url.local:8123",
        "read_cluster_label_2" => "no-url",
        "read_cluster_url_2" => ""
      }

      assert {:ok, assembled} = ReadClusterUrlsComponent.assemble_read_only_urls(config)
      assert assembled["read_only_urls"] == %{"reporting" => "http://reporting.local:8123"}
    end

    test "returns an empty map when there are no read-cluster form params" do
      assert {:ok, %{"read_only_urls" => %{}}} =
               ReadClusterUrlsComponent.assemble_read_only_urls(%{
                 "url" => "http://ingest.local:8123"
               })
    end
  end

  describe "handle_event/3 sync fallback" do
    test "logs a warning with the backend id when the payload shape is unexpected" do
      socket = %Socket{assigns: %{backend: %Backend{id: 123}, __changed__: %{}}}

      event =
        capture_log_event(fn ->
          assert {:noreply, ^socket} =
                   ReadClusterUrlsComponent.handle_event(
                     "sync",
                     %{"unexpected" => "shape"},
                     socket
                   )
        end)

      assert %{level: :warning, meta: %{backend_id: 123}} = event
      assert {:string, message} = event.msg

      assert IO.iodata_to_binary(message) =~
               "Unexpected sync payload shape in ReadClusterUrlsComponent"
    end

    test "logs a nil backend id when the backend has not been created yet" do
      socket = %Socket{assigns: %{backend: nil, __changed__: %{}}}

      event =
        capture_log_event(fn ->
          assert {:noreply, _socket} = ReadClusterUrlsComponent.handle_event("sync", %{}, socket)
        end)

      assert %{meta: %{backend_id: nil}} = event
    end
  end

  defmodule CaptureHandler do
    def log(event, %{config: %{pid: pid}}), do: send(pid, {:log_event, event})
  end

  defp capture_log_event(fun) do
    handler_id = :"read_cluster_urls_test_#{System.unique_integer([:positive])}"

    :ok =
      :logger.add_handler(handler_id, __MODULE__.CaptureHandler, %{
        level: :all,
        config: %{pid: self()}
      })

    try do
      fun.()
    after
      :logger.remove_handler(handler_id)
    end

    assert_receive {:log_event, event}, 1_000
    event
  end
end
