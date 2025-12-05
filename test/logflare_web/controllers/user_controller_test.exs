defmodule LogflareWeb.UserControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Users

  setup do
    insert(:plan)
    :ok
  end

  describe "UserController update" do
    setup do
      u1 = insert(:user, bigquery_dataset_id: "test_dataset_id_1", billing_enabled: true)
      u2 = insert(:user, bigquery_dataset_id: "test_dataset_id_2")
      t1 = insert(:team, user: u1)
      t2 = insert(:team, user: u2)
      u1 = %{u1 | team: t1}
      u2 = %{u2 | team: t2}

      {:ok, users: [u1, u2]}
    end

    test "of restricted fields fails", %{
      conn: conn,
      users: [u1 | _]
    } do
      nope_token = TestUtils.random_string()
      nope_api_quota = 1337
      nope_user_id = 1

      reject(Users.Cache, :get_by, 1)

      conn =
        conn
        |> login_user(u1)
        |> put(
          ~p"/account/edit",
          %{
            "user" => %{
              "name" => u1.name,
              "token" => nope_token,
              "api_quota" => nope_api_quota,
              "id" => nope_user_id
            }
          }
        )

      s1_new = Users.get_by(id: u1.id)

      refute conn.assigns[:changeset]
      refute s1_new.token == nope_token
      refute s1_new.api_quota == nope_api_quota
      refute s1_new.id == nope_user_id
      assert redirected_to(conn, 302) =~ ~p"/account/edit"
    end

    test "of allowed fields succeeds", %{
      conn: conn,
      users: [u1 | _]
    } do
      new_email = TestUtils.gen_email()

      new = %{
        email: new_email,
        provider: TestUtils.random_string(),
        email_preferred: TestUtils.gen_email(),
        name: TestUtils.random_string(),
        image: "https://#{TestUtils.random_string()}.com",
        email_me_product: true,
        phone: "(555) 12345",
        system_monitoring: true
      }

      reject(Users.Cache, :get_by, 1)

      conn =
        conn
        |> login_user(u1)
        |> put(~p"/account/edit", %{"user" => new})

      comparable_keys = Map.keys(new) -- [:email, :email_preferred]

      s1_new = Users.get_by(id: u1.id)

      refute conn.assigns[:changeset]
      assert s1_new.email == String.downcase(new.email)
      assert s1_new.email_preferred == String.downcase(new.email_preferred)

      assert s1_new |> Map.from_struct() |> Map.take(comparable_keys) ==
               Map.take(new, comparable_keys)

      assert html_response(conn, 302) =~ ~p"/account/edit"

      conn =
        conn
        |> recycle()
        |> Plug.Test.init_test_session(%{current_email: String.downcase(new_email)})
        |> get(~p"/account/edit")

      assert conn.assigns.user.email == String.downcase(new_email)

      assert conn.assigns.user.system_monitoring
    end

    test "of bigquery_project_id resets all user tables if premium user", %{
      conn: conn,
      users: [u1 | _]
    } do
      pid = self()
      u1_id = u1.id
      bq_project_id = "logflare-byob-for-test"
      insert(:plan, name: "Lifetime")
      insert(:billing_account, user: u1, lifetime_plan: true)

      expect(Logflare.Google.BigQuery, :create_dataset, fn user_id,
                                                           _dataset_id,
                                                           _location,
                                                           project_id ->
        send(pid, {{:user_id, :project_id}, {user_id, project_id}})
        {:ok, []}
      end)

      expect(Logflare.Sources.Source.Supervisor, :reset_all_user_sources, fn user ->
        send(pid, {:user_id, user.id})
        :ok
      end)

      conn =
        conn
        |> login_user(u1)
        |> put(
          ~p"/account/edit",
          %{
            "user" => %{
              "bigquery_project_id" => bq_project_id
            }
          }
        )

      TestUtils.retry_assert(fn ->
        u_id = to_string(u1_id)
        assert_received {{:user_id, :project_id}, {^u_id, ^bq_project_id}}
      end)

      TestUtils.retry_assert(fn ->
        assert_received {:user_id, ^u1_id}
      end)

      refute conn.assigns[:changeset]
      assert redirected_to(conn, 302) =~ ~p"/account/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Account updated!"
    end
  end

  describe "UserController delete" do
    setup %{conn: conn} do
      user = insert(:user)
      [conn: login_user(conn, user), user: user]
    end

    test "succeeds", %{conn: conn} do
      expect(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, _project_number, [body: _body] ->
          {:ok, ""}
        end
      )

      conn = delete(conn, ~p"/account")
      assert redirected_to(conn, 302) =~ ~p"/auth/login?user_deleted=true"
    end
  end

  describe "UserController change_owner" do
    test "owner cannot transfer ownership to team member from another team", %{conn: conn} do
      owner_a = insert(:user)
      _team_a = insert(:team, user: owner_a)

      owner_b = insert(:user)
      team_b = insert(:team, user: owner_b)
      victim_team_user = insert(:team_user, team: team_b, email: "victim@example.com")

      conn =
        conn
        |> login_user(owner_a)
        |> put(~p"/account/edit/owner", %{"user" => %{"team_user_id" => victim_team_user.id}})

      assert redirected_to(conn, 302) =~ "/account/edit"

      assert Phoenix.Flash.get(conn.assigns.flash, :error) ==
               "Not authorized to transfer ownership to this team member"

      assert Logflare.TeamUsers.get_team_user!(victim_team_user.id)
    end

    test "owner can transfer ownership to their own team member", %{conn: conn} do
      owner = insert(:user)
      team = insert(:team, user: owner)
      team_user = insert(:team_user, team: team, email: "member@example.com")

      conn =
        conn
        |> login_user(owner)
        |> put(~p"/account/edit/owner", %{"user" => %{"team_user_id" => team_user.id}})

      assert redirected_to(conn, 302) =~ "/account/edit"
      assert Phoenix.Flash.get(conn.assigns.flash, :info) == "Owner successfully changed!"
      refute Logflare.TeamUsers.get_team_user(team_user.id)
    end
  end

  describe "partner-provisioned user" do
    setup %{conn: conn} do
      user = insert(:user)
      insert(:partner, users: [user])
      [conn: login_user(conn, user)]
    end

    test "bug: should be able to delete a partner-provisioned account", %{conn: conn} do
      expect(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, _project_number, [body: _body] ->
          {:ok, ""}
        end
      )

      conn = delete(conn, ~p"/account")
      assert redirected_to(conn, 302) =~ ~p"/auth/login"
    end
  end
end
