defmodule Logflare.Backends.SpoolGcpAdaptersTest do
  use ExUnit.Case, async: true

  import Mimic

  alias GoogleApi.PubSub.V1.Api.Projects, as: PubSubApi
  alias GoogleApi.PubSub.V1.Model.AcknowledgeRequest
  alias GoogleApi.PubSub.V1.Model.ModifyAckDeadlineRequest
  alias GoogleApi.PubSub.V1.Model.PublishRequest
  alias GoogleApi.PubSub.V1.Model.PubsubMessage
  alias GoogleApi.PubSub.V1.Model.PullRequest
  alias GoogleApi.PubSub.V1.Model.PullResponse
  alias GoogleApi.PubSub.V1.Model.ReceivedMessage
  alias GoogleApi.Storage.V1.Api.Objects
  alias Logflare.Backends.Spool.Queue.PubSub
  alias Logflare.Backends.Spool.Storage.GCS

  # build_conn/0 returns a static "local-dev-token" connection when
  # :goth, :json is nil (the case in tests). No Goth process needed.

  describe "Storage.GCS.put/4" do
    test "sends raw binary via media upload with correct content-type" do
      bucket = "test-bucket"
      key = "0/abc.ndjson.gz"
      body = "binary-data"

      stub(Tesla, :request, fn _conn, opts ->
        assert opts[:method] == :post
        assert String.starts_with?(opts[:url], "http"), "URL must be absolute, got: #{opts[:url]}"
        assert String.contains?(opts[:url], "/upload/storage/v1/b/test-bucket/o")
        assert opts[:query][:uploadType] == "media"
        assert opts[:query][:name] == key
        assert {"Content-Type", "application/x-ndjson"} in opts[:headers]
        assert opts[:body] == body
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      assert {:ok, ^key} =
               GCS.put(bucket, key, body, headers: %{"content-type" => "application/x-ndjson"})
    end

    test "defaults content-type to application/octet-stream when not provided" do
      stub(Tesla, :request, fn _conn, opts ->
        assert String.starts_with?(opts[:url], "http"), "URL must be absolute, got: #{opts[:url]}"
        assert {"Content-Type", "application/octet-stream"} in opts[:headers]
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      GCS.put("b", "k", "data", [])
    end

    test "returns {:error, {status, body}} on non-2xx response" do
      stub(Tesla, :request, fn _conn, _opts ->
        {:ok, %Tesla.Env{status: 403, body: "Forbidden"}}
      end)

      assert {:error, {403, "Forbidden"}} = GCS.put("b", "k", "data", [])
    end

    test "returns {:error, reason} on connection error" do
      stub(Tesla, :request, fn _conn, _opts ->
        {:error, :econnrefused}
      end)

      assert {:error, :econnrefused} = GCS.put("b", "k", "data", [])
    end
  end

  describe "Storage.GCS.get/2" do
    test "returns binary body on success" do
      stub(Objects, :storage_objects_get, fn _conn, bucket, key, [alt: "media"] ->
        assert bucket == "test-bucket"
        assert key == "0/abc.ndjson.gz"
        {:ok, %Tesla.Env{body: "file-contents"}}
      end)

      assert {:ok, "file-contents"} = GCS.get("test-bucket", "0/abc.ndjson.gz")
    end

    test "returns {:error, reason} on API failure" do
      stub(Objects, :storage_objects_get, fn _conn, _bucket, _key, _opts ->
        {:error, %Tesla.Env{status: 404, body: "Not Found"}}
      end)

      assert {:error, _} = GCS.get("b", "missing-key")
    end
  end

  describe "Queue.PubSub.publish/2" do
    test "base64-encodes body and publishes to topic" do
      topic = "projects/logflare/topics/logflare-spool"
      body = ~s({"file_key":"0/abc.ndjson.gz","event_count":10})

      stub(PubSubApi, :pubsub_projects_topics_publish, fn _conn, t, [body: req] ->
        assert t == topic
        assert %PublishRequest{messages: [%PubsubMessage{data: encoded}]} = req
        assert Base.decode64!(encoded) == body
        {:ok, %{messageIds: ["msg-1"]}}
      end)

      assert :ok = PubSub.publish(topic, body)
    end

    test "returns {:error, reason} on API failure" do
      stub(PubSubApi, :pubsub_projects_topics_publish, fn _conn, _topic, _opts ->
        {:error, %Tesla.Env{status: 500}}
      end)

      assert {:error, _} = PubSub.publish("projects/p/topics/t", "body")
    end
  end

  describe "Queue.PubSub.receive/2" do
    test "decodes base64 message data and normalizes to %{id:, body:}" do
      subscription = "projects/logflare/subscriptions/logflare-spool-sub"
      raw_body = ~s({"file_key":"0/abc.ndjson.gz"})
      encoded = Base.encode64(raw_body)

      stub(PubSubApi, :pubsub_projects_subscriptions_pull, fn _conn, sub, [body: req] ->
        assert sub == subscription
        assert %PullRequest{maxMessages: 1} = req

        {:ok,
         %PullResponse{
           receivedMessages: [
             %ReceivedMessage{
               ackId: "ack-id-1",
               message: %PubsubMessage{data: encoded}
             }
           ]
         }}
      end)

      assert {:ok, [%{id: "ack-id-1", body: ^raw_body}]} = PubSub.receive(subscription, [])
    end

    test "returns empty list when no messages" do
      stub(PubSubApi, :pubsub_projects_subscriptions_pull, fn _conn, _sub, _opts ->
        {:ok, %PullResponse{receivedMessages: nil}}
      end)

      assert {:ok, []} = PubSub.receive("projects/p/subscriptions/s", [])
    end
  end

  describe "Queue.PubSub.ack/2" do
    test "acknowledges with the given ack_id" do
      subscription = "projects/logflare/subscriptions/logflare-spool-sub"

      stub(PubSubApi, :pubsub_projects_subscriptions_acknowledge, fn _conn, sub, [body: req] ->
        assert sub == subscription
        assert %AcknowledgeRequest{ackIds: ["ack-id-1"]} = req
        {:ok, %{}}
      end)

      assert :ok = PubSub.ack(subscription, "ack-id-1")
    end
  end

  describe "Queue.PubSub.nack/2" do
    test "sets ackDeadlineSeconds to 0 to redeliver immediately" do
      subscription = "projects/logflare/subscriptions/logflare-spool-sub"

      stub(PubSubApi, :pubsub_projects_subscriptions_modify_ack_deadline, fn _conn, sub,
                                                                              [body: req] ->
        assert sub == subscription
        assert %ModifyAckDeadlineRequest{ackIds: ["ack-id-1"], ackDeadlineSeconds: 0} = req
        {:ok, %{}}
      end)

      assert :ok = PubSub.nack(subscription, "ack-id-1")
    end
  end
end
