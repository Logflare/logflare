defmodule Logflare.TeamUsersTest do
  @moduledoc false
  use Logflare.DataCase, async: true

  alias Logflare.Repo
  alias Logflare.TeamUsers
  alias Logflare.Teams.Team
  alias Logflare.TeamUsers.TeamUser

  describe "list_team_users/0" do
    test "returns all team users" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)
      team_user_id = team_user.id

      assert [%TeamUser{id: ^team_user_id}] = TeamUsers.list_team_users()
    end

    test "returns empty list when no team users exist" do
      assert TeamUsers.list_team_users() == []
    end
  end

  describe "list_team_users_by/1" do
    test "returns team users matching keyword filter" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)
      team_user_id = team_user.id

      assert [%TeamUser{id: ^team_user_id}] = TeamUsers.list_team_users_by(email: team_user.email)
    end

    test "returns empty list when no match" do
      assert TeamUsers.list_team_users_by(email: "nobody@example.com") == []
    end
  end

  describe "list_team_users_by_and_preload/1" do
    test "returns team users with team preloaded" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)
      team_user_id = team_user.id

      assert [%TeamUser{id: ^team_user_id, team: %Team{}}] =
               TeamUsers.list_team_users_by_and_preload(email: team_user.email)
    end
  end

  describe "get_team_user!/1" do
    test "returns the team user for a valid id" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert TeamUsers.get_team_user!(team_user.id).id == team_user.id
    end

    test "raises Ecto.NoResultsError for unknown id" do
      assert_raise Ecto.NoResultsError, fn -> TeamUsers.get_team_user!(0) end
    end
  end

  describe "get_team_user/1" do
    test "returns the team user for a valid id" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert TeamUsers.get_team_user(team_user.id).id == team_user.id
    end

    test "returns nil for unknown id" do
      assert TeamUsers.get_team_user(0) == nil
    end
  end

  describe "get_team_user_by/1" do
    test "returns team user matching keyword" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert TeamUsers.get_team_user_by(email: team_user.email).id == team_user.id
    end

    test "returns nil when no match" do
      assert TeamUsers.get_team_user_by(email: "nope@example.com") == nil
    end
  end

  describe "get_team_user_and_preload/1" do
    test "returns team user with team preloaded" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)
      team_user_id = team_user.id

      assert %TeamUser{id: ^team_user_id, team: %Team{}} =
               TeamUsers.get_team_user_and_preload(team_user.id)
    end

    test "returns nil for unknown id" do
      assert TeamUsers.get_team_user_and_preload(0) == nil
    end
  end

  describe "create_team_user/2" do
    @valid_attrs %{
      email: "user_#{System.unique_integer()}@example.com",
      provider: "google",
      provider_uid: "uid_#{System.unique_integer()}"
    }

    test "creates team user with valid attrs" do
      team = insert(:team)

      assert {:ok, %TeamUser{} = team_user} = TeamUsers.create_team_user(team.id, @valid_attrs)
      assert team_user.email == @valid_attrs.email
      assert team_user.provider == @valid_attrs.provider
      assert team_user.role == :user
      assert team_user.team_id == team.id
    end

    test "returns error changeset with invalid attrs" do
      team = insert(:team)

      assert {:error, %Ecto.Changeset{}} = TeamUsers.create_team_user(team.id, %{})
    end
  end

  describe "update_team_user/2" do
    test "updates team user with valid attrs" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)
      new_email = "updated_#{System.unique_integer()}@example.com"

      assert {:ok, updated} =
               TeamUsers.update_team_user(team_user, %{email: new_email, role: "admin"})

      assert updated.email == new_email
      assert updated.role == team_user.role, "should not change"
    end

    test "returns error changeset with invalid attrs" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert {:error, %Ecto.Changeset{}} = TeamUsers.update_team_user(team_user, %{email: nil})
    end
  end

  describe "update_team_user_profile/2" do
    test "updates display and contact preferences" do
      team_user = insert(:team_user)

      assert {:ok, updated} =
               TeamUsers.update_team_user_profile(team_user, %{
                 name: "Profile name",
                 email_preferred: "CONTACT@EXAMPLE.TEST",
                 phone: "+1 (555) 123-4567",
                 image: "https://example.test/avatar.png",
                 email_me_product: false
               })

      updated = Repo.reload!(updated)
      assert updated.name == "Profile name"
      assert updated.email_preferred == "contact@example.test"
      assert updated.phone == "+1 (555) 123-4567"
      assert updated.image == "https://example.test/avatar.png"
      refute updated.email_me_product
    end

    test "ignores identity fields with atom and string keys" do
      team_user = insert(:team_user) |> Repo.reload!()
      other_team = insert(:team)

      identity_fields = [
        :id,
        :team_id,
        :email,
        :provider,
        :provider_uid,
        :token,
        :valid_google_account
      ]

      attrs = %{
        id: 0,
        team_id: other_team.id,
        email: "operator@example.test",
        provider: "email",
        provider_uid: "forged-identity",
        token: Ecto.UUID.generate(),
        valid_google_account: true,
        name: "Safe profile name"
      }

      string_attrs = Map.new(attrs, fn {key, value} -> {Atom.to_string(key), value} end)

      for params <- [attrs, string_attrs] do
        assert {:ok, updated} = TeamUsers.update_team_user_profile(team_user, params)

        assert Map.take(Repo.reload!(updated), identity_fields) ==
                 Map.take(team_user, identity_fields)

        assert updated.name == "Safe profile name"
      end
    end

    test "rejects invalid editable fields without writing them" do
      team_user = insert(:team_user)

      assert {:error, %Ecto.Changeset{valid?: false}} =
               TeamUsers.update_team_user_profile(team_user, %{name: %{invalid: true}})

      assert Repo.reload!(team_user).name == team_user.name
    end
  end

  test "trusted authentication updates retain identity synchronization" do
    team_user = insert(:team_user)
    token = Ecto.UUID.generate()

    assert {:ok, updated} =
             TeamUsers.update_team_user(team_user, %{
               email: "TRUSTED@EXAMPLE.TEST",
               provider: "github",
               provider_uid: "trusted-provider-identity",
               token: token,
               valid_google_account: true
             })

    assert updated.email == "trusted@example.test"
    assert updated.provider == "github"
    assert updated.provider_uid == "trusted-provider-identity"
    assert updated.token == token
    assert updated.valid_google_account
  end

  describe "update_team_role/2" do
    test "updates team user role with valid attrs" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert {:ok, updated} = TeamUsers.update_team_role(team_user, %{role: :admin})
      assert updated.role == :admin
    end

    test "returns error changeset with invalid role" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert {:error, %Ecto.Changeset{} = changeset} =
               TeamUsers.update_team_role(team_user, %{role: :owner})

      assert [role: {"is invalid", _meta}] = changeset.errors
    end
  end

  describe "delete_team_user/1" do
    test "deletes the team user" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert {:ok, %TeamUser{}} = TeamUsers.delete_team_user(team_user)
      assert TeamUsers.get_team_user(team_user.id) == nil
    end
  end

  describe "change_team_user/1" do
    test "returns an Ecto.Changeset" do
      team = insert(:team)
      team_user = insert(:team_user, team: team)

      assert %Ecto.Changeset{} = TeamUsers.change_team_user(team_user)
    end
  end

  describe "insert_or_update_team_user/2" do
    setup do
      insert(:plan, name: "Free", limit_team_users_limit: 1)
      team = insert(:team)

      %{team: team}
    end

    @valid_attrs %{
      email: "user_#{System.unique_integer()}@example.com",
      provider: "google",
      provider_uid: "uid_#{System.unique_integer()}"
    }

    test "creates a new team user when no match exists and under limit", %{team: team} do
      assert {:ok, %TeamUser{}} = TeamUsers.insert_or_update_team_user(team, @valid_attrs)
    end

    test "updates existing team user matched by provider_uid", %{team: team} do
      valid_attrs = Map.put(@valid_attrs, :team, team)
      team_user = insert(:team_user, valid_attrs)

      updated_attrs = Map.put(valid_attrs, :email, "new_#{System.unique_integer()}@example.com")

      assert {:ok, updated} = TeamUsers.insert_or_update_team_user(team, updated_attrs)
      assert updated.id == team_user.id
      assert updated.email == updated_attrs.email
    end

    test "updates existing team user matched by email when provider_uid differs", %{team: team} do
      valid_attrs = Map.put(@valid_attrs, :team, team)
      team_user = insert(:team_user, valid_attrs)

      attrs_new_uid = Map.put(valid_attrs, :provider_uid, "new_uid_#{System.unique_integer()}")

      assert {:ok, updated} = TeamUsers.insert_or_update_team_user(team, attrs_new_uid)
      assert updated.id == team_user.id
    end

    test "returns {:error, :limit_reached} when at plan limit", %{team: team} do
      insert(:team_user, team: team)

      assert {:error, :limit_reached} = TeamUsers.insert_or_update_team_user(team, @valid_attrs)
    end
  end
end
