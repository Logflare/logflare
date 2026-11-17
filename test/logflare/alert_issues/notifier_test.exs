defmodule Logflare.AlertIssues.NotifierTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues.Notifier

  @event %{
    "event_id" => "6f1d2c1e-1b9e-4a55-9d0c-0d8b4f3a1e11",
    "type" => "alert_issue.opened",
    "occurred_at" => "2026-11-17T12:00:00Z",
    "policy" => %{"name" => "ops", "receivers" => ["ops-webhook"]},
    "alert_issue" => %{"alert_issue_id" => "a1", "title" => "RLS disabled"}
  }

  setup do
    previous = Application.get_env(:logflare, Notifier)

    Application.put_env(:logflare, Notifier,
      url: "http://control-plane.test/api/alert-issues/notifications",
      secret: "s3cret"
    )

    on_exit(fn -> Application.put_env(:logflare, Notifier, previous) end)
  end

  test "is enabled only when a notifier URL is configured" do
    assert Notifier.enabled?()
    Application.put_env(:logflare, Notifier, url: nil, secret: "s3cret")
    refute Notifier.enabled?()
  end

  test "signs the timestamp and raw body with HMAC-SHA256" do
    expected =
      :crypto.mac(:hmac, :sha256, "s3cret", "1700000000.{}") |> Base.encode16(case: :lower)

    assert Notifier.sign("s3cret", 1_700_000_000, "{}") == "v1=" <> expected
  end

  test "posts the event as signed JSON" do
    test_pid = self()

    stub(Tesla.Adapter.Finch, :call, fn %Tesla.Env{} = env, _opts ->
      send(test_pid, {:request, env})
      {:ok, %Tesla.Env{env | status: 202, body: ""}}
    end)

    assert {:ok, 202} = Notifier.deliver(@event)

    assert_receive {:request, env}
    assert env.method == :post
    assert env.url == "http://control-plane.test/api/alert-issues/notifications"
    assert Jason.decode!(env.body) == @event

    headers = Map.new(env.headers)
    assert headers["content-type"] == "application/json"
    assert headers["x-logflare-event-id"] == @event["event_id"]
    timestamp = String.to_integer(headers["x-logflare-timestamp"])
    assert headers["x-logflare-signature"] == Notifier.sign("s3cret", timestamp, env.body)
  end

  test "returns non-2xx statuses and transport errors as errors" do
    stub(Tesla.Adapter.Finch, :call, fn %Tesla.Env{} = env, _opts ->
      {:ok, %Tesla.Env{env | status: 500, body: "boom"}}
    end)

    assert {:error, {:http_status, 500}} = Notifier.deliver(@event)

    stub(Tesla.Adapter.Finch, :call, fn _env, _opts -> {:error, :econnrefused} end)
    assert {:error, :econnrefused} = Notifier.deliver(@event)
  end
end
