defmodule Logflare.Google.CloudResourceManagerTest do
  use Logflare.DataCase

  alias GoogleApi.CloudResourceManager.V1.Api.Projects
  alias GoogleApi.CloudResourceManager.V1.Model
  alias GoogleApi.CloudResourceManager.V1.Model.Binding
  alias GoogleApi.CloudResourceManager.V1.Model.Policy
  alias Logflare.Google.CloudResourceManager

  setup do
    google_configs = Map.new(Application.get_env(:logflare, Logflare.Google))
    expected_members = setup_test_state()

    on_exit(fn ->
      Application.put_env(:logflare, Logflare.Google, Map.to_list(google_configs))
    end)

    %{
      google_configs: google_configs,
      expected_members: expected_members
    }
  end

  describe "set_iam_policy/0" do
    setup do
      stub(GoogleApi.IAM.V1.Api.Projects, :iam_projects_service_accounts_list, fn
        _conn, "projects/" <> _project_id, _opts ->
          {:ok, %{accounts: [], nextPageToken: nil}}
      end)

      :ok
    end

    test "appends roles for paying users, paying team users and service accounts", %{
      google_configs: google_configs,
      expected_members: expected_members
    } do
      fake_iam(%Policy{bindings: [], etag: "etag-0"})

      assert :ok = CloudResourceManager.set_iam_policy(async: false)

      bindings = current_policy().bindings
      assert Enum.all?(bindings, &match?(%Binding{members: [_ | _], role: "" <> _}, &1))

      job_user_members = members_for(bindings, "roles/bigquery.jobUser")

      for member <- expected_members do
        assert member in job_user_members
      end

      assert ("user:" <> google_configs.project_viewer) in members_for(bindings, "roles/viewer")

      assert ("serviceAccount:" <> google_configs.cloud_sql_client_sa) in members_for(
               bindings,
               "roles/cloudsql.client"
             )
    end

    test "preserves bindings and members added outside of logflare", %{
      expected_members: [first_member | _]
    } do
      fake_iam(%Policy{
        etag: "etag-0",
        bindings: [
          %Binding{role: "roles/cloudsql.client", members: ["serviceAccount:proxy@x.com"]},
          %Binding{role: "roles/owner", members: ["user:owner@x.com"]},
          %Binding{role: "roles/bigquery.jobUser", members: ["user:manual@x.com"]}
        ]
      })

      assert :ok = CloudResourceManager.set_iam_policy(async: false)

      bindings = current_policy().bindings
      assert "serviceAccount:proxy@x.com" in members_for(bindings, "roles/cloudsql.client")
      assert "user:owner@x.com" in members_for(bindings, "roles/owner")
      assert "user:manual@x.com" in members_for(bindings, "roles/bigquery.jobUser")
      assert first_member in members_for(bindings, "roles/bigquery.jobUser")

      assert bindings
             |> Enum.map(& &1.role)
             |> Enum.frequencies()
             |> Map.values()
             |> Enum.all?(&(&1 == 1))
    end

    test "sends the fetched etag with each write" do
      fake_iam(%Policy{bindings: [], etag: "etag-0"})

      assert :ok = CloudResourceManager.set_iam_policy(async: false)

      assert_received {:set_policy, %Policy{etag: "etag-0"}}
    end

    test "does not write when every binding is already present" do
      fake_iam(%Policy{bindings: [], etag: "etag-0"})
      assert :ok = CloudResourceManager.set_iam_policy(async: false)
      flush_set_policies()

      assert :ok = CloudResourceManager.set_iam_policy(async: false)

      refute_received {:set_policy, _}
    end

    test "does not write when the policy cannot be fetched" do
      stub(Projects, :cloudresourcemanager_projects_get_iam_policy, fn _, _, _ ->
        {:error, %Tesla.Env{status: 500, body: Jason.encode!(%{error: %{message: "boom"}})}}
      end)

      reject(&Projects.cloudresourcemanager_projects_set_iam_policy/3)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, _} = CloudResourceManager.set_iam_policy(async: false)
        end)

      assert log =~ "[error]"
      refute log =~ "successful"
    end

    test "uses group: prefix as-is for project viewer binding", %{
      google_configs: google_configs
    } do
      Application.put_env(
        :logflare,
        Logflare.Google,
        Map.to_list(%{google_configs | project_viewer: "group:support@mile.cloud"})
      )

      fake_iam(%Policy{bindings: []})
      CloudResourceManager.set_iam_policy(async: false)

      viewers = members_for(current_policy().bindings, "roles/viewer")
      assert "group:support@mile.cloud" in viewers
      refute "user:support@mile.cloud" in viewers
    end

    test "omits cloudsql.client binding when cloud_sql_client_sa is not configured", %{
      google_configs: google_configs
    } do
      Application.put_env(
        :logflare,
        Logflare.Google,
        Map.to_list(%{google_configs | cloud_sql_client_sa: nil})
      )

      fake_iam(%Policy{bindings: []})
      CloudResourceManager.set_iam_policy(async: false)

      assert members_for(current_policy().bindings, "roles/cloudsql.client") == []
    end

    test "excludes a deleted user, keeps adding the rest, and does not retry it", %{
      expected_members: expected_members
    } do
      user =
        insert(:user,
          provider: "google",
          valid_google_account: true,
          billing_account: insert(:billing_account, lifetime_plan: true),
          team: insert(:team)
        )

      team_user =
        insert(:team_user, email: user.email, provider: "google", valid_google_account: true)

      fake_iam(%Policy{bindings: []}, %{
        ("user:" <> user.email) => "User #{user.email} does not exist."
      })

      log =
        ExUnit.CaptureLog.capture_log(fn -> CloudResourceManager.set_iam_policy(async: false) end)

      assert log =~ "[error]"
      assert log =~ "marked 2 account(s) as invalid"
      refute log =~ "successful"

      refute Logflare.Repo.get!(Logflare.User, user.id).valid_google_account
      refute Logflare.Repo.get!(Logflare.TeamUsers.TeamUser, team_user.id).valid_google_account

      job_user_members = members_for(current_policy().bindings, "roles/bigquery.jobUser")

      for member <- expected_members do
        assert member in job_user_members
      end

      flush_set_policies()
      CloudResourceManager.set_iam_policy(async: false)
      refute_received {:set_policy, _}
    end

    test "excludes a principal that is not a user account" do
      team_user = insert(:team_user, provider: "google", valid_google_account: true)

      paying_owner =
        insert(:user,
          provider: "google",
          valid_google_account: true,
          billing_account: insert(:billing_account, lifetime_plan: true),
          team: team_user.team
        )

      fake_iam(%Policy{bindings: []}, %{
        ("user:" <> team_user.email) => ~s(Principal #{team_user.email} is of type "group".)
      })

      ExUnit.CaptureLog.capture_log(fn -> CloudResourceManager.set_iam_policy(async: false) end)

      refute Logflare.Repo.get!(Logflare.TeamUsers.TeamUser, team_user.id).valid_google_account

      job_user_members = members_for(current_policy().bindings, "roles/bigquery.jobUser")
      assert ("user:" <> paying_owner.email) in job_user_members
      refute ("user:" <> team_user.email) in job_user_members
    end
  end

  describe "append_managed_sa_to_all_iam_projects/1" do
    setup do
      original_pool_size =
        Application.get_env(:logflare, :bigquery_backend_adaptor)[
          :managed_service_account_pool_size
        ]

      Application.put_env(:logflare, :bigquery_backend_adaptor,
        managed_service_account_pool_size: 1
      )

      on_exit(fn ->
        Application.put_env(:logflare, :bigquery_backend_adaptor,
          managed_service_account_pool_size: original_pool_size
        )
      end)

      stub(GoogleApi.IAM.V1.Api.Projects, :iam_projects_service_accounts_list, fn
        _conn, "projects/" <> _project_id, _opts ->
          {:ok,
           %{
             accounts: [
               %GoogleApi.IAM.V1.Model.ServiceAccount{
                 email: "logflare-managed-0@some-project.iam.gserviceaccount.com",
                 name:
                   "projects/some-project/serviceAccounts/logflare-managed-0@some-project.iam.gserviceaccount.com"
               }
             ],
             nextPageToken: nil
           }}
      end)

      :ok
    end

    test "calls set_iam_policy for primary project and each additional project" do
      user =
        insert(:user,
          bigquery_project_id: "primary-project",
          bigquery_enable_managed_service_accounts: true,
          bigquery_additional_projects: "extra-a, extra-b"
        )

      pid = self()

      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_get_iam_policy,
        fn _, project_id, [body: _body] ->
          send(pid, {:get_policy, project_id})
          {:ok, %Model.Policy{bindings: []}}
        end
      )

      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, project_id, [body: body] ->
          send(pid, {:set_policy, project_id})
          {:ok, body.policy}
        end
      )

      result = CloudResourceManager.append_managed_sa_to_all_iam_projects(user)

      assert {:ok, _} = result.primary
      assert length(result.additional) == 2
      assert Enum.all?(result.additional, fn {_, res} -> match?({:ok, _}, res) end)

      assert_receive {:set_policy, "primary-project"}
      assert_receive {:set_policy, "extra-a"}
      assert_receive {:set_policy, "extra-b"}
    end

    test "with nil additional_projects only processes primary" do
      user =
        insert(:user,
          bigquery_project_id: "primary-project",
          bigquery_enable_managed_service_accounts: true,
          bigquery_additional_projects: nil
        )

      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_get_iam_policy,
        fn _, _project_id, [body: _body] ->
          {:ok, %Model.Policy{bindings: []}}
        end
      )

      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, _project_id, [body: body] ->
          {:ok, body.policy}
        end
      )

      result = CloudResourceManager.append_managed_sa_to_all_iam_projects(user)

      assert {:ok, _} = result.primary
      assert [] = result.additional
    end

    test "with empty string additional_projects only processes primary" do
      user =
        insert(:user,
          bigquery_project_id: "primary-project",
          bigquery_enable_managed_service_accounts: true,
          bigquery_additional_projects: "  ,  "
        )

      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_get_iam_policy,
        fn _, _project_id, [body: _body] ->
          {:ok, %Model.Policy{bindings: []}}
        end
      )

      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, _project_id, [body: body] ->
          {:ok, body.policy}
        end
      )

      result = CloudResourceManager.append_managed_sa_to_all_iam_projects(user)

      assert {:ok, _} = result.primary
      assert [] = result.additional
    end
  end

  defp setup_test_state do
    stripe_id = "testing"
    insert(:plan, name: "Free")
    insert(:plan, name: "Legacy")
    insert(:plan, name: "Lifetime")
    insert(:plan, name: "Stripe", stripe_id: stripe_id)

    insert(:user_without_billing_account)
    insert(:user_without_stripe_subscription)
    insert(:user_with_wrong_stripe_sub_content)
    insert(:user_with_lifetime_account_non_google)
    user_with_lifetime_account = insert(:user_with_lifetime_account)
    user_with_legacy_account = insert(:user_with_legacy_account)
    user_with_stripe_subscription = insert(:user_with_stripe_subscription, stripe_id: stripe_id)

    team = insert(:team)

    team_owner_user =
      insert(:user,
        provider: "google",
        valid_google_account: true,
        billing_account: insert(:billing_account, lifetime_plan: true),
        team: team
      )

    user_non_owner_paid_account =
      insert(:team_user,
        team: team,
        email: insert(:user).email,
        provider: "google",
        valid_google_account: true
      )

    [
      user_with_lifetime_account,
      user_with_legacy_account,
      user_with_stripe_subscription,
      team_owner_user,
      user_non_owner_paid_account
    ]
    |> Enum.sort_by(& &1.updated_at, {:desc, Date})
    |> Enum.map(&"user:#{&1.email}")
  end

  defp fake_iam(%Policy{} = policy, rejected \\ %{}) do
    Process.put(:fake_policy, policy)
    Process.put(:fake_rejected, rejected)

    stub(Projects, :cloudresourcemanager_projects_get_iam_policy, fn _, _, _ ->
      {:ok, current_policy()}
    end)

    stub(Projects, :cloudresourcemanager_projects_set_iam_policy, &fake_set/3)
  end

  defp fake_set(_conn, _project, body: %Model.SetIamPolicyRequest{policy: policy}) do
    send(self(), {:set_policy, policy})
    members = Enum.flat_map(policy.bindings, & &1.members)

    case Enum.find(Process.get(:fake_rejected), fn {member, _} -> member in members end) do
      {_member, message} ->
        {:error, %Tesla.Env{status: 400, body: Jason.encode!(%{error: %{message: message}})}}

      nil ->
        new_policy = %{policy | etag: "etag-#{System.unique_integer([:positive])}"}
        Process.put(:fake_policy, new_policy)
        {:ok, new_policy}
    end
  end

  defp current_policy, do: Process.get(:fake_policy)

  defp members_for(bindings, role) do
    bindings |> Enum.filter(&(&1.role == role)) |> Enum.flat_map(& &1.members)
  end

  defp flush_set_policies do
    receive do
      {:set_policy, _} -> flush_set_policies()
    after
      0 -> :ok
    end
  end
end
