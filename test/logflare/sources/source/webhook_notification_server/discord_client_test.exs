defmodule Logflare.Sources.Source.WebhookNotificationServer.DiscordClientTest do
  use Logflare.DataCase, async: false

  import Mimic

  alias Logflare.Sources.Source.WebhookNotificationServer.DiscordClient

  setup :set_mimic_global

  setup do
    insert(:plan)
    user = insert(:user)

    source =
      insert(:source,
        user: user,
        webhook_notification_url: "https://discord.com/api/webhooks/test"
      )

    [source: source]
  end

  defp stub_tesla_ok(test_pid, ref) do
    stub(Tesla, :post, fn _client, _url, payload ->
      send(test_pid, {ref, payload})
      {:ok, %Tesla.Env{status: 200}}
    end)
  end

  defp make_log_event(source, body_overrides) do
    timestamp_us = DateTime.utc_now() |> DateTime.to_unix(:microsecond)

    body =
      Map.merge(
        %{"timestamp" => timestamp_us, "event_message" => "default message"},
        body_overrides
      )

    %Logflare.LogEvent{body: body, source_id: source.id}
  end

  describe "post/4" do
    test "json stringifies body when event_message is nil", %{source: source} do
      ref = make_ref()
      stub_tesla_ok(self(), ref)

      timestamp_us = DateTime.utc_now() |> DateTime.to_unix(:microsecond)
      body = %{"timestamp" => timestamp_us, "event_message" => nil, "foo" => "bar"}
      event = %Logflare.LogEvent{body: body, source_id: source.id}
      client = DiscordClient.new()

      assert {:ok, _} = DiscordClient.post(client, source, 1, [event])
      assert_receive {^ref, payload}, 1000

      [field] = payload.embeds |> hd() |> Map.get(:fields)
      assert field.value =~ ~s("foo")
      assert field.value =~ ~s("bar")
    end

    test "json stringifies body when event_message is absent", %{source: source} do
      ref = make_ref()
      stub_tesla_ok(self(), ref)

      timestamp_us = DateTime.utc_now() |> DateTime.to_unix(:microsecond)
      body = %{"timestamp" => timestamp_us, "count" => 42}
      event = %Logflare.LogEvent{body: body, source_id: source.id}
      client = DiscordClient.new()

      assert {:ok, _} = DiscordClient.post(client, source, 1, [event])
      assert_receive {^ref, payload}, 1000

      [field] = payload.embeds |> hd() |> Map.get(:fields)
      assert field.value =~ ~s("count")
      assert field.value =~ "42"
    end

    test "includes event_message when present", %{source: source} do
      ref = make_ref()
      stub_tesla_ok(self(), ref)

      event = make_log_event(source, %{"event_message" => "hello world"})
      client = DiscordClient.new()

      assert {:ok, _} = DiscordClient.post(client, source, 1, [event])
      assert_receive {^ref, payload}, 1000

      [field] = payload.embeds |> hd() |> Map.get(:fields)
      assert field.value == "```hello world```"
    end

    test "truncates long event_message to 1018 chars", %{source: source} do
      ref = make_ref()
      stub_tesla_ok(self(), ref)

      long_message = String.duplicate("x", 2000)
      event = make_log_event(source, %{"event_message" => long_message})
      client = DiscordClient.new()

      assert {:ok, _} = DiscordClient.post(client, source, 1, [event])
      assert_receive {^ref, %{embeds: [%{fields: fields} | _]}}, 1000

      # value is "```<message>```" so message portion is value minus 6 backtick chars
      assert String.length(fields |> hd() |> Map.get(:value)) < 1030
    end
  end
end
