defmodule LogflareWeb.Auth.UnsubscribeControllerTest do
  use LogflareWeb.ConnCase, async: true

  alias Logflare.Auth
  alias Logflare.Sources

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    {:ok, user: user, source: source}
  end

  describe "unsubscribe/2" do
    test "unsubscribes team user from schema updates with valid token", %{
      conn: conn,
      source: source
    } do
      team_user = insert(:team_user, email: "teamuser@example.com")

      {:ok, source} =
        Sources.update_source(source, %{
          notifications: %{
            team_user_ids_for_schema_updates: [to_string(team_user.id)]
          }
        })

      token = Auth.gen_email_token(team_user.email)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/#{token}?type=team_user")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Unsubscribed!"

      assert updated_source = Sources.get(source.id)
      assert updated_source.notifications.team_user_ids_for_schema_updates == []
    end

    test "unsubscribes user from schema update notifications with valid token", %{
      conn: conn,
      source: source,
      user: user
    } do
      {:ok, source} =
        Sources.update_source(source, %{
          notifications: %{
            user_schema_update_notifications: true
          }
        })

      token = Auth.gen_email_token(user.email)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/#{token}?type=user")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Unsubscribed!"

      assert updated_source = Sources.get(source.id)
      assert updated_source.notifications.user_schema_update_notifications == false
    end

    test "unsubscribes user from emails notifications with valid token", %{
      conn: conn,
      source: source,
      user: user
    } do
      {:ok, source} =
        Sources.update_source(source, %{
          notifications: %{
            user_schema_update_notifications: true
          }
        })

      token = Auth.gen_email_token(user.email)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/#{token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Unsubscribed!"

      assert updated_source = Sources.get(source.id)
      assert updated_source.notifications.user_email_notifications == false
    end

    test "returns error for expired token", %{conn: conn, source: source} do
      invalid_token = "expired_token"

      pid = self()

      Phoenix.Token
      |> expect(:verify, fn _, _, token, _ ->
        send(pid, {:verified_token, token})
        {:error, :expired}
      end)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/#{invalid_token}")

      TestUtils.retry_assert(fn ->
        assert_received {:verified_token, ^invalid_token}
      end)

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "That link is expired!"
    end

    test "returns error for invalid token", %{conn: conn, source: source} do
      invalid_token = "invalid_token_123"

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/#{invalid_token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Bad link!"
    end

    for type <- ["user", "team_user"] do
      test "returns error for expired token for #{type}", %{conn: conn, source: source} do
        type = unquote(type)
        invalid_token = "expired_token"

        pid = self()

        Phoenix.Token
        |> expect(:verify, fn _, _, token, _ ->
          send(pid, {:verified_token, token})
          {:error, :expired}
        end)

        conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/#{invalid_token}?type=#{type}")

        TestUtils.retry_assert(fn ->
          assert_received {:verified_token, ^invalid_token}
        end)

        assert redirected_to(conn) == ~p"/auth/login"
        assert Phoenix.Flash.get(conn.assigns.flash, :error) == "That link is expired!"
      end

      test "returns error for invalid token for #{type}", %{conn: conn, source: source} do
        type = unquote(type)
        invalid_token = "invalid_token_123"

        conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/#{invalid_token}?type=#{type}")

        assert redirected_to(conn) == ~p"/auth/login"
        assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Bad link!"
      end
    end
  end

  describe "unsubscribe_stranger/2" do
    test "unsubscribes stranger email from notifications with valid token", %{
      conn: conn,
      source: source
    } do
      stranger_email = "stranger@example.com"

      {:ok, source} =
        Sources.update_source(source, %{
          notifications: %{
            other_email_notifications:
              "stranger@example.com, another@example.com, another_one@example.com"
          }
        })

      token = Auth.gen_email_token(stranger_email)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/stranger/#{token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Unsubscribed!"

      assert updated_source = Sources.get(source.id)

      assert updated_source.notifications.other_email_notifications ==
               "another@example.com, another_one@example.com"
    end

    test "handles nil other_email_notifications", %{conn: conn, source: source} do
      stranger_email = "stranger@example.com"

      {:ok, source} =
        Sources.update_source(source, %{
          notifications: %{
            other_email_notifications: nil
          }
        })

      token = Auth.gen_email_token(stranger_email)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/stranger/#{token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Unsubscribed!"

      assert updated_source = Sources.get(source.id)
      assert updated_source.notifications.other_email_notifications == nil
    end

    test "returns error for expired token", %{conn: conn, source: source} do
      invalid_token = "expired_token"

      pid = self()

      Phoenix.Token
      |> expect(:verify, fn _, _, token, _ ->
        send(pid, {:verified_token, token})
        {:error, :expired}
      end)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/stranger/#{invalid_token}")

      TestUtils.retry_assert(fn ->
        assert_received {:verified_token, ^invalid_token}
      end)

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "That link is expired!"
    end

    test "returns error for invalid token", %{conn: conn, source: source} do
      invalid_token = "invalid_token_123"

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/stranger/#{invalid_token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Bad link!"
    end
  end

  describe "unsubscribe_team_user/2" do
    setup do
      team_user = insert(:team_user, email: "teamuser@example.com")

      %{team_user: team_user}
    end

    test "unsubscribes team user from email notifications with valid token", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      {:ok, source} =
        Sources.update_source(source, %{
          notifications: %{
            team_user_ids_for_email: [to_string(team_user.id)]
          }
        })

      token = Auth.gen_email_token(team_user.email)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/team-member/#{token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Unsubscribed!"

      assert updated_source = Sources.get(source.id)
      assert updated_source.notifications.team_user_ids_for_email == []
    end

    test "removes only matching team user from notifications list", %{
      conn: conn,
      source: source,
      team_user: team_user
    } do
      other_team_user = insert(:team_user, email: "other@example.com")

      {:ok, source} =
        Sources.update_source(source, %{
          notifications: %{
            team_user_ids_for_email: [to_string(team_user.id), to_string(other_team_user.id)]
          }
        })

      token = Auth.gen_email_token(team_user.email)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/team-member/#{token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Unsubscribed!"

      assert updated_source = Sources.get(source.id)

      assert updated_source.notifications.team_user_ids_for_email == [
               to_string(other_team_user.id)
             ]
    end

    test "returns error for expired token", %{conn: conn, source: source} do
      invalid_token = "expired_token"

      pid = self()

      Phoenix.Token
      |> expect(:verify, fn _, _, token, _ ->
        send(pid, {:verified_token, token})
        {:error, :expired}
      end)

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/team-member/#{invalid_token}")

      TestUtils.retry_assert(fn ->
        assert_received {:verified_token, ^invalid_token}
      end)

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "That link is expired!"
    end

    test "returns error for invalid token", %{conn: conn, source: source} do
      invalid_token = "invalid_token_123"

      conn = get(conn, ~p"/sources/#{source.id}/unsubscribe/team-member/#{invalid_token}")

      assert redirected_to(conn) == ~p"/auth/login"
      assert Phoenix.Flash.get(conn.assigns.flash, :error) == "Bad link!"
    end
  end
end
