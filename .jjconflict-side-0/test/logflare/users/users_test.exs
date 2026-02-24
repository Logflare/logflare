defmodule Logflare.UsersTest do
  use Logflare.DataCase, async: true

  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.Users

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    source = Sources.get_by(token: source.token)
    user = Users.preload_defaults(user)

    {:ok, user: user, source: source}
  end

  describe "Users.list_ingesting_users/1" do
    test "lists ingesting users based on source activity" do
      assert [] = Users.list_ingesting_users(limit: 500)
      user = insert(:user)
      insert(:source, user: user, log_events_updated_at: NaiveDateTime.utc_now())
      assert [_] = Users.list_ingesting_users(limit: 500)
    end
  end

  describe "Users.list_users/1" do
    test "lists all users created by a partner" do
      partner_other = insert(:partner)
      insert_list(3, :user, partner: partner_other)
      partner = insert(:partner)
      user = insert(:user, partner: partner)

      assert [user_result] = Users.list_users(partner_id: partner.id)
      assert user_result.id == user.id
    end

    test "list_users/1" do
      insert(:user)
      insert(:user, metadata: %{"a" => "123"})
      assert [_] = Users.list_users(metadata: %{"a" => "123"})
    end
  end

  test "delete_user/1" do
    user = insert(:user)
    alert = insert(:alert, user: user)
    source = insert(:source, user: user)
    endpoint = insert(:endpoint, user: user)

    expect(
      GoogleApi.CloudResourceManager.V1.Api.Projects,
      :cloudresourcemanager_projects_set_iam_policy,
      fn _, _project_number, [body: _body] ->
        {:ok,
         %Tesla.Env{
           status: 200,
           body: ""
         }}
      end
    )

    assert {:ok, _} = Users.delete_user(user)

    refute Repo.reload(alert)
    refute Repo.reload(source)
    refute Repo.reload(endpoint)
    refute Repo.reload(user)
  end

  test "users_count/0 returns user count" do
    assert Users.count_users() == 1
    insert(:user)
    assert Users.count_users() == 2
  end

  describe "user_changeset" do
    test "adds api_key to changeset changes if data does not have it" do
      params = %{
        "email" => TestUtils.gen_email(),
        "provider_uid" => TestUtils.gen_uuid(),
        "provider" => "email",
        "token" => TestUtils.gen_uuid()
      }

      result = Users.user_changeset(%User{}, params)
      assert result.changes.api_key
    end

    test "does not add api_key to changeset changes if data has it" do
      user = insert(:user)
      result = Users.user_changeset(user, %{name: TestUtils.random_string()})
      refute Map.has_key?(result.changes, :api_key)
    end
  end

  describe "get_by/1" do
    test "get user by id", %{user: u1} do
      assert %User{} = fetched = Users.get_by(id: u1.id)
      assert fetched.bigquery_dataset_id
      assert fetched.bigquery_project_id
    end

    test "get user by api_key", %{user: right_user} do
      left_user = Users.get_by(api_key: right_user.api_key)
      assert left_user.id == right_user.id
      assert Users.get_by(api_key: "nil") == nil
    end
  end

  describe "update_user_allowed/2" do
    test "changes user information", %{user: u1} do
      user = Users.get_by(api_key: u1.api_key)
      email = TestUtils.random_string()

      {:ok, user} = Users.update_user_allowed(user, %{"email_preferred" => email})
      assert Users.get_by(api_key: u1.api_key).email_preferred == String.downcase(email)

      {:ok, _user} = Users.update_user_allowed(user, %{"email_preferred" => nil})
      assert Users.get_by(api_key: u1.api_key).email_preferred == nil
    end

    test "can create system sources and update cache info", %{user: user} do
      {:ok, user} = Users.update_user_allowed(user, %{"system_monitoring" => true})

      system_sources = Sources.list_system_sources_by_user(user)

      assert Enum.any?(system_sources, &(&1.system_source_type == :metrics))
      assert Enum.any?(system_sources, &(&1.system_source_type == :logs))

      assert Users.Cache.get(user.id).system_monitoring

      Users.update_user_allowed(user, %{"system_monitoring" => false})

      refute Users.Cache.get(user.id).system_monitoring
    end
  end

  describe "insert_user/1" do
    test "inserts new user" do
      params = %{"email" => TestUtils.random_string(), "provider" => "email"}
      assert {:ok, _user} = Users.insert_user(params)
    end

    test "inserts new user and generates provider_uid and token automatically" do
      params = %{"email" => TestUtils.random_string(), "provider" => "email"}

      assert {:ok, user} = Users.insert_user(params)
      assert user.provider_uid
      assert user.token
    end

    test "insert_user/1 with metadata" do
      assert {:ok, user} =
               Users.insert_user(%{
                 "email" => TestUtils.random_string(),
                 "provider" => "email",
                 "metadata" => %{"some" => "value"}
               })

      assert user.metadata["some"] == "value"
    end
  end

  describe "insert_or_update_user/1" do
    test "if user exists with provider_uid, updates it", %{user: u1} do
      name = TestUtils.random_string()

      params = %{
        name: name,
        provider: "email",
        email: u1.email,
        provider_uid: u1.provider_uid,
        token: u1.token
      }

      {:ok_found_user, user} = Users.insert_or_update_user(params)
      assert user.name == name
      assert user.id == u1.id
    end

    test "if provider_uid does not exist but user exists, search by email and update", %{user: u1} do
      name = TestUtils.random_string()

      params = %{
        name: name,
        provider: "email",
        email: u1.email,
        provider_uid: Ecto.UUID.generate(),
        token: u1.token
      }

      {:ok_found_user, user} = Users.insert_or_update_user(params)
      assert user.name == name
      assert user.id == u1.id
    end

    test "if no user exists with given email or provider_uid creates a new one with given params" do
      params = %{
        name: TestUtils.random_string(),
        provider: "email",
        email: TestUtils.random_string(),
        provider_uid: Ecto.UUID.generate(),
        token: Ecto.UUID.generate()
      }

      assert {:ok, _user} = Users.insert_or_update_user(params)
    end

    test "if there are missing params, returns the error changeset" do
      params = %{
        name: TestUtils.random_string(),
        provider: "email",
        token: Ecto.UUID.generate()
      }

      assert {:error, "Missing email or provider_uid"} = Users.insert_or_update_user(params)
    end
  end
end
