defmodule Logflare.VercelTest do
  use Logflare.DataCase

  alias Logflare.Vercel

  describe "vercel_auths" do
    alias Logflare.Vercel.Auth

    @valid_attrs %{access_token: "some access_token", installation_id: "some installation_id", team_id: "some team_id", token_type: "some token_type", vercel_user_id: "some vercel_user_id"}
    @update_attrs %{access_token: "some updated access_token", installation_id: "some updated installation_id", team_id: "some updated team_id", token_type: "some updated token_type", vercel_user_id: "some updated vercel_user_id"}
    @invalid_attrs %{access_token: nil, installation_id: nil, team_id: nil, token_type: nil, vercel_user_id: nil}

    def auth_fixture(attrs \\ %{}) do
      {:ok, auth} =
        attrs
        |> Enum.into(@valid_attrs)
        |> Vercel.create_auth()

      auth
    end

    test "list_vercel_auths/0 returns all vercel_auths" do
      auth = auth_fixture()
      assert Vercel.list_vercel_auths() == [auth]
    end

    test "get_auth!/1 returns the auth with given id" do
      auth = auth_fixture()
      assert Vercel.get_auth!(auth.id) == auth
    end

    test "create_auth/1 with valid data creates a auth" do
      assert {:ok, %Auth{} = auth} = Vercel.create_auth(@valid_attrs)
      assert auth.access_token == "some access_token"
      assert auth.installation_id == "some installation_id"
      assert auth.team_id == "some team_id"
      assert auth.token_type == "some token_type"
      assert auth.vercel_user_id == "some vercel_user_id"
    end

    test "create_auth/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Vercel.create_auth(@invalid_attrs)
    end

    test "update_auth/2 with valid data updates the auth" do
      auth = auth_fixture()
      assert {:ok, %Auth{} = auth} = Vercel.update_auth(auth, @update_attrs)
      assert auth.access_token == "some updated access_token"
      assert auth.installation_id == "some updated installation_id"
      assert auth.team_id == "some updated team_id"
      assert auth.token_type == "some updated token_type"
      assert auth.vercel_user_id == "some updated vercel_user_id"
    end

    test "update_auth/2 with invalid data returns error changeset" do
      auth = auth_fixture()
      assert {:error, %Ecto.Changeset{}} = Vercel.update_auth(auth, @invalid_attrs)
      assert auth == Vercel.get_auth!(auth.id)
    end

    test "delete_auth/1 deletes the auth" do
      auth = auth_fixture()
      assert {:ok, %Auth{}} = Vercel.delete_auth(auth)
      assert_raise Ecto.NoResultsError, fn -> Vercel.get_auth!(auth.id) end
    end

    test "change_auth/1 returns a auth changeset" do
      auth = auth_fixture()
      assert %Ecto.Changeset{} = Vercel.change_auth(auth)
    end
  end
end
