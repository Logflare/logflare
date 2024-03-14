defmodule Logflare.Backends.WebhookAdaptorTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor

  @subject Logflare.Backends.Adaptor.WebhookAdaptor

  setup :set_mimic_global

  setup do
    stub(Goth, :fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend, type: :webhook, sources: [source], config: %{url: "https://example.com"})

    pid = start_supervised!({@subject, {source, backend}})
    [pid: pid, backend: backend, source: source]
  end

  test "ingest/2", %{source: source} do
    @subject.Client
    |> expect(:send, fn _, _ -> %Tesla.Env{} end)

    le = build(:log_event, source: source)
    assert :ok = Backends.ingest_logs([le], source)
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
