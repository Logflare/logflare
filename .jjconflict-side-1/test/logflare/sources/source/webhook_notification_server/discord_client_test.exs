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

  describe "post/4" do
    test "json stringifies body when event_message is nil or absent", %{source: source} do
      for body <- [
            %{"timestamp" => 1_000_000, "event_message" => nil, "foo" => "bar"},
            %{"timestamp" => 1_000_000, "count" => 42}
          ] do
        ref = make_ref()

        stub(Tesla, :post, fn _client, _url, payload ->
          send(self(), {ref, payload})
          {:ok, %Tesla.Env{status: 200}}
        end)

        event = %Logflare.LogEvent{body: body, source_id: source.id}
        client = DiscordClient.new()

        assert {:ok, _} = DiscordClient.post(client, source, 1, [event])
        assert_receive {^ref, payload}, 1000

        [field] = payload.embeds |> hd() |> Map.get(:fields)
        assert field.value =~ ~s("timestamp")
      end
    end

    test "includes event_message when present", %{source: source} do
      ref = make_ref()

      stub(Tesla, :post, fn _client, _url, payload ->
        send(self(), {ref, payload})
        {:ok, %Tesla.Env{status: 200}}
      end)

      event = build(:log_event, source: source, event_message: "hello world")
      client = DiscordClient.new()

      assert {:ok, _} = DiscordClient.post(client, source, 1, [event])
      assert_receive {^ref, payload}, 1000

      [field] = payload.embeds |> hd() |> Map.get(:fields)
      assert field.value == "```hello world```"
    end

    test "truncates long event_message to under 1030 chars", %{source: source} do
      ref = make_ref()

      stub(Tesla, :post, fn _client, _url, payload ->
        send(self(), {ref, payload})
        {:ok, %Tesla.Env{status: 200}}
      end)

      event = build(:log_event, source: source, event_message: String.duplicate("x", 2000))
      client = DiscordClient.new()

      assert {:ok, _} = DiscordClient.post(client, source, 1, [event])
      assert_receive {^ref, %{embeds: [%{fields: fields} | _]}}, 1000

      assert String.length(fields |> hd() |> Map.get(:value)) < 1030
    end
  end
end
