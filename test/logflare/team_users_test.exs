defmodule Logflare.TeamUsersTest do
  use Logflare.DataCase

  alias Logflare.TeamUsers

  describe "team_users" do
    alias Logflare.TeamUsers.TeamUser

    @valid_attrs %{email: "some email", email_me_product: true, email_preferred: "some email_preferred", image: "some image", name: "some name", phone: "some phone", provider: "some provider", provider_uid: "some provider_uid", token: "some token", valid_google_account: true}
    @update_attrs %{email: "some updated email", email_me_product: false, email_preferred: "some updated email_preferred", image: "some updated image", name: "some updated name", phone: "some updated phone", provider: "some updated provider", provider_uid: "some updated provider_uid", token: "some updated token", valid_google_account: false}
    @invalid_attrs %{email: nil, email_me_product: nil, email_preferred: nil, image: nil, name: nil, phone: nil, provider: nil, provider_uid: nil, token: nil, valid_google_account: nil}

    def team_user_fixture(attrs \\ %{}) do
      {:ok, team_user} =
        attrs
        |> Enum.into(@valid_attrs)
        |> TeamUsers.create_team_user()

      team_user
    end

    test "list_team_users/0 returns all team_users" do
      team_user = team_user_fixture()
      assert TeamUsers.list_team_users() == [team_user]
    end

    test "get_team_user!/1 returns the team_user with given id" do
      team_user = team_user_fixture()
      assert TeamUsers.get_team_user!(team_user.id) == team_user
    end

    test "create_team_user/1 with valid data creates a team_user" do
      assert {:ok, %TeamUser{} = team_user} = TeamUsers.create_team_user(@valid_attrs)
      assert team_user.email == "some email"
      assert team_user.email_me_product == true
      assert team_user.email_preferred == "some email_preferred"
      assert team_user.image == "some image"
      assert team_user.name == "some name"
      assert team_user.phone == "some phone"
      assert team_user.provider == "some provider"
      assert team_user.provider_uid == "some provider_uid"
      assert team_user.token == "some token"
      assert team_user.valid_google_account == true
    end

    test "create_team_user/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = TeamUsers.create_team_user(@invalid_attrs)
    end

    test "update_team_user/2 with valid data updates the team_user" do
      team_user = team_user_fixture()
      assert {:ok, %TeamUser{} = team_user} = TeamUsers.update_team_user(team_user, @update_attrs)
      assert team_user.email == "some updated email"
      assert team_user.email_me_product == false
      assert team_user.email_preferred == "some updated email_preferred"
      assert team_user.image == "some updated image"
      assert team_user.name == "some updated name"
      assert team_user.phone == "some updated phone"
      assert team_user.provider == "some updated provider"
      assert team_user.provider_uid == "some updated provider_uid"
      assert team_user.token == "some updated token"
      assert team_user.valid_google_account == false
    end

    test "update_team_user/2 with invalid data returns error changeset" do
      team_user = team_user_fixture()
      assert {:error, %Ecto.Changeset{}} = TeamUsers.update_team_user(team_user, @invalid_attrs)
      assert team_user == TeamUsers.get_team_user!(team_user.id)
    end

    test "delete_team_user/1 deletes the team_user" do
      team_user = team_user_fixture()
      assert {:ok, %TeamUser{}} = TeamUsers.delete_team_user(team_user)
      assert_raise Ecto.NoResultsError, fn -> TeamUsers.get_team_user!(team_user.id) end
    end

    test "change_team_user/1 returns a team_user changeset" do
      team_user = team_user_fixture()
      assert %Ecto.Changeset{} = TeamUsers.change_team_user(team_user)
    end
  end
end
