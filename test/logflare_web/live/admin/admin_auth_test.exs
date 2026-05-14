defmodule LogflareWeb.AdminLive.AdminAuthTest do
  use Logflare.DataCase, async: true

  alias LogflareWeb.AdminLive.AdminAuth

  # Minimal socket — redirect/2 only touches the :redirected field.
  defp bare_socket, do: %Phoenix.LiveView.Socket{}

  setup do
    insert(:plan)
    :ok
  end

  test "on_mount :ensure_admin allows an admin through" do
    admin = insert(:user, admin: true)

    assert {:cont, _socket} =
             AdminAuth.on_mount(
               :ensure_admin,
               %{},
               %{"current_email" => admin.email},
               bare_socket()
             )
  end

  test "on_mount :ensure_admin redirects a non-admin to /" do
    user = insert(:user, admin: false)

    assert {:halt, socket} =
             AdminAuth.on_mount(
               :ensure_admin,
               %{},
               %{"current_email" => user.email},
               bare_socket()
             )

    assert socket.redirected == {:redirect, %{to: "/"}}
  end

  test "on_mount :ensure_admin redirects when session has no email" do
    assert {:halt, socket} =
             AdminAuth.on_mount(:ensure_admin, %{}, %{}, bare_socket())

    assert socket.redirected == {:redirect, %{to: "/"}}
  end

  test "on_mount :ensure_admin re-checks DB so a revoked admin is redirected" do
    admin = insert(:user, admin: true)
    # Revoke directly in DB (simulates revocation after login)
    Logflare.Repo.update!(Ecto.Changeset.change(admin, admin: false))

    assert {:halt, socket} =
             AdminAuth.on_mount(
               :ensure_admin,
               %{},
               %{"current_email" => admin.email},
               bare_socket()
             )

    assert socket.redirected == {:redirect, %{to: "/"}}
  end
end
