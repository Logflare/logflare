defmodule LogflareWeb.LogControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.Backends.Adaptor.WebhookAdaptor

  @valid %{"log_entry" => "valid log entry"}

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id, v2_pipeline: true)
    _plan = insert(:plan, name: "Free")

    source_backend =
      insert(:source_backend, source_id: source.id, type: :webhook, config: %{url: "some url"})

    # stub out rate limiting logic for now
    # TODO: remove once rate limiting logic is refactored
    LogflareWeb.Plugs.RateLimiter
    |> stub(:call, fn x, _ -> x end)

    {:ok, source: source, user: user, source_backend: source_backend}
  end

  test "valid ingestion", %{conn: conn, source: source, user: user} do
    WebhookAdaptor
    |> expect(:ingest, fn _, _ -> :ok end)

    conn =
      conn
      |> put_req_header("x-api-key", user.api_key)
      |> post(Routes.log_path(conn, :create, source: source.token), @valid)

    assert json_response(conn, 200) == %{"message" => "Logged!"}
    :timer.sleep(1500)
  end
end
