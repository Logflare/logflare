defmodule Logflare.Backends.GoogleAnalyticsAdaptorTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{LogEvent, Backends.Adaptor.GoogleAnalyticsAdaptor}
  setup :set_mimic_global

  setup do
    source_backend =
      insert(:source_backend,
        type: :google_analytics,
        config: %{
          measurement_id: "G-1234",
          api_secret: "1234",
          client_id_path: "metadata.client_id",
          event_name_paths: "name"
        }
      )

    pid = start_supervised!({GoogleAnalyticsAdaptor, source_backend})
    {:ok, pid: pid}
  end

  test "ingest/2", %{pid: pid} do
    GoogleAnalyticsAdaptor.Client
    |> expect(:send, fn _a, body ->
      assert body["client_id"] == "my-id"
      [event] = body["events"]
      assert event["name"] == "my-name"
      %Tesla.Env{}
    end)

    assert :ok =
             GoogleAnalyticsAdaptor.ingest(pid, [
               %LogEvent{
                 body: %{
                   "name" => "my-name",
                   "metadata" => %{"client_id" => "my-id"}
                 }
               }
             ])

    :timer.sleep(1_500)
  end

  test "cast_and_validate_config/1" do
    for valid <- [
          %{
            measurement_id: "G-1234",
            api_secret: "1234",
            client_id_path: "metadata.client_id",
            event_name_paths: "name,testing"
          }
        ] do
      assert %Ecto.Changeset{valid?: true} =
               GoogleAnalyticsAdaptor.cast_and_validate_config(valid),
             "valid: #{inspect(valid)}"
    end

    for invalid <- [
          %{},
          %{api_secret: "123"},
          %{api_secret: "123", client_id_path: "metadata.client"},
          %{api_secret: "123", client_id_path: "metadata.client", measureemnt_id: 123}
        ] do
      assert %Ecto.Changeset{valid?: false} =
               GoogleAnalyticsAdaptor.cast_and_validate_config(invalid),
             "invalid: #{inspect(invalid)}"
    end
  end
end
