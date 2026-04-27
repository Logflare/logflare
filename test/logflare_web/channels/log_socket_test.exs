defmodule LogflareWeb.LogSocketTest do
  use LogflareWeb.ChannelCase

  alias Logflare.Backends
  alias LogflareWeb.LogChannel
  alias LogflareWeb.LogSocket

  setup do
    insert(:plan)
    :ok
  end

  describe "access token authentication" do
    test "access token owner can connect and ingest into their own source" do
      user = insert(:user)
      source = insert(:source, user: user)
      token = insert(:access_token, resource_owner: user)

      {:ok, socket} = Phoenix.ChannelTest.connect(LogSocket, %{"access_token" => token.token})
      {:ok, _, socket} = subscribe_and_join(socket, LogChannel, "logs:#{source.token}")

      Backends
      |> expect(:ingest_logs, fn batch, ingest_source ->
        assert [%{"message" => "access-token-log"}] = batch
        assert ingest_source.id == source.id
        {:ok, 1}
      end)

      Phoenix.ChannelTest.push(socket, "batch", %{"batch" => [%{"message" => "access-token-log"}]})

      Phoenix.ChannelTest.assert_push("batch", %{message: "Handled batch"})

      leave(socket)
    end

    test "access token cannot connect with an unknown token" do
      assert :error =
               Phoenix.ChannelTest.connect(LogSocket, %{"access_token" => "not-a-real-token"})
    end

    test "access token owner cannot ingest into another user's source" do
      victim = insert(:user)
      attacker = insert(:user)
      victim_source = insert(:source, user: victim)
      token = insert(:access_token, resource_owner: attacker)

      {:ok, socket} = Phoenix.ChannelTest.connect(LogSocket, %{"access_token" => token.token})

      Backends
      |> reject(:ingest_logs, 2)

      assert {:error, %{reason: "Not authorized!"}} =
               subscribe_and_join(socket, LogChannel, "logs:#{victim_source.token}")
    end
  end

  describe "log socket authorization" do
    test "owner can ingest into their own source using legacy api key" do
      user = insert(:user)
      source = insert(:source, user: user)

      {:ok, socket} = Phoenix.ChannelTest.connect(LogSocket, %{"api_key" => user.api_key})
      {:ok, _, socket} = subscribe_and_join(socket, LogChannel, "logs:#{source.token}")

      Backends
      |> expect(:ingest_logs, fn batch, ingest_source ->
        assert [%{"message" => "owned-log"}] = batch
        assert ingest_source.id == source.id
        {:ok, 1}
      end)

      Phoenix.ChannelTest.push(socket, "batch", %{"batch" => [%{"message" => "owned-log"}]})
      Phoenix.ChannelTest.assert_push("batch", %{message: "Handled batch"})

      leave(socket)
    end

    test "legacy log socket api key cannot ingest into another user's source" do
      victim = insert(:user)
      attacker = insert(:user)
      victim_source = insert(:source, user: victim)

      {:ok, socket} =
        Phoenix.ChannelTest.connect(LogSocket, %{"api_key" => attacker.api_key})

      Backends
      |> reject(:ingest_logs, 2)

      assert {:error, %{reason: "Not authorized!"}} =
               subscribe_and_join(socket, LogChannel, "logs:#{victim_source.token}")
    end

    test "admin cannot ingest into any source" do
      admin = insert(:user, admin: true)
      other = insert(:user)
      other_source = insert(:source, user: other)

      {:ok, socket} = Phoenix.ChannelTest.connect(LogSocket, %{"api_key" => admin.api_key})

      Backends
      |> reject(:ingest_logs, 2)

      assert {:error, %{reason: "Not authorized!"}} =
               subscribe_and_join(socket, LogChannel, "logs:#{other_source.token}")
    end
  end
end
