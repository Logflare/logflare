defmodule LogflareWeb.UserSocketTest do
  use LogflareWeb.ChannelCase

  alias Logflare.Auth
  alias Logflare.Sources.Source.ChannelTopics
  alias LogflareWeb.SourceChannel
  alias LogflareWeb.UserSocket

  setup do
    insert(:plan)
    :ok
  end

  describe "SourceChannelpublic_token authorization" do
    test "a signed public token can subscribe to its own source channel" do
      user = insert(:user)

      source =
        insert(:source, user: user, public_token: Logflare.TestUtils.random_string())

      signed = Auth.gen_public_source_token(source.public_token)

      {:ok, socket} =
        Phoenix.ChannelTest.connect(UserSocket, %{
          "token" => "undefined",
          "public_token" => signed
        })

      assert {:ok, _, socket} =
               subscribe_and_join(socket, SourceChannel, "source:#{source.token}")

      ChannelTopics.broadcast_new(build(:log_event, source: source, message: "own-public-msg"))

      event = "source:#{source.token}:new"
      assert_push(^event, %{body: %{"event_message" => "own-public-msg"}})

      leave(socket)
    end

    test "a signed public token cannot subscribe to a different source channel" do
      public_user = insert(:user)
      victim_user = insert(:user)

      public_source =
        insert(:source, user: public_user, public_token: Logflare.TestUtils.random_string())

      victim_source = insert(:source, user: victim_user)

      signed_public_token = Auth.gen_public_source_token(public_source.public_token)

      {:ok, socket} =
        Phoenix.ChannelTest.connect(UserSocket, %{
          "token" => "undefined",
          "public_token" => signed_public_token
        })

      assert {:error, %{reason: "Not authorized!"}} =
               subscribe_and_join(socket, SourceChannel, "source:#{victim_source.token}")
    end
  end

  describe "user token salt namespacing" do
    test "a signed invite token cannot be replayed as a user socket token" do
      victim_user = insert(:user)

      invite_owner = insert(:user)
      invite_team = insert(:team, id: victim_user.id, user: invite_owner)
      signed_invite_token = Auth.gen_email_token(invite_team.id)

      assert :error =
               Phoenix.ChannelTest.connect(UserSocket, %{
                 "token" => signed_invite_token,
                 "public_token" => "undefined"
               })
    end

    test "a public source token cannot be replayed as a user socket token" do
      victim_user = insert(:user)
      _victim_source = insert(:source, user: victim_user)

      # Sign the victim's id under the public-source salt - should NOT verify as a user token.
      forged = Auth.gen_public_source_token(victim_user.id)

      assert :error =
               Phoenix.ChannelTest.connect(UserSocket, %{
                 "token" => forged,
                 "public_token" => "undefined"
               })
    end

    test "a legitimately signed user socket token connects" do
      user = insert(:user)
      token = Auth.gen_user_socket_token(user.id)

      assert {:ok, _socket} =
               Phoenix.ChannelTest.connect(UserSocket, %{
                 "token" => token,
                 "public_token" => "undefined"
               })
    end
  end

  test "both undefined should fail" do
    assert :error =
             Phoenix.ChannelTest.connect(UserSocket, %{
               "token" => "undefined",
               "public_token" => "undefined"
             })
  end
end
