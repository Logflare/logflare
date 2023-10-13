defmodule Logflare.Backends.WebhookAdaptorTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.LogEvent
  alias Logflare.Backends.Adaptor

  @subject Logflare.Backends.Adaptor.WebhookAdaptor

  setup :set_mimic_global

  setup do
    source_backend =
      insert(:source_backend, type: :webhook, config: %{url: "https://example.com"})

    pid = start_supervised!({@subject, source_backend})
    {:ok, pid: pid}
  end

  test "ingest/2", %{pid: pid} do
    @subject.Client
    |> expect(:send, fn _, _ -> %Tesla.Env{} end)

    assert :ok = @subject.ingest(pid, [%LogEvent{}])
    :timer.sleep(1_500)
  end

  test "cast_and_validate_config/1" do
    for valid <- [
          %{url: "http://example.com"},
          %{url: "https://example.com"}
        ] do
      assert %Ecto.Changeset{valid?: true} = Adaptor.cast_and_validate_config(@subject, valid),
             "valid: #{inspect(valid)}"
    end

    for invalid <- [
          %{},
          %{url: nil},
          %{url: "htp://invalid.com"}
        ] do
      assert %Ecto.Changeset{valid?: false} = Adaptor.cast_and_validate_config(@subject, invalid),
             "invalid: #{inspect(invalid)}"
    end
  end
end
