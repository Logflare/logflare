defmodule Logflare.Google.CloudResourceManagerTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Google.CloudResourceManager
  alias GoogleApi.CloudResourceManager.V1.Model.Binding

  setup do
    google_configs = Map.new(Application.get_env(:logflare, Logflare.Google))
    expected_members = setup_test_state()

    %{
      google_configs: google_configs,
      expected_members: expected_members
    }
  end

  describe "set_iam_policy/0" do
    setup do
      # Mock IAM API calls for listing existing service accounts
      stub(GoogleApi.IAM.V1.Api.Projects, :iam_projects_service_accounts_list, fn
        _conn, "projects/" <> _project_id, _opts ->
          {:ok, %{accounts: [], nextPageToken: nil}}
      end)

      :ok
    end

    test "request body sends expected roles for paying users, paying team users and service accounts",
         %{
           google_configs: google_configs,
           expected_members: expected_members
         } do
      pid = self()

      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, project_number, [body: body] ->
          assert project_number == google_configs.project_number
          send(pid, body.policy.bindings)
          {:ok, ""}
        end
      )

      CloudResourceManager.set_iam_policy(async: false)

      assert_received [_ | _] = bindings

      assert Enum.all?(bindings, fn binding ->
               %Binding{members: [_ | _], role: "" <> _} = binding
             end)

      members_list = Enum.flat_map(bindings, & &1.members)

      for member <- expected_members do
        assert Enum.member?(members_list, member)
      end
    end

    test "sets user as invalid google account if user does not exist" do
      user = insert(:user, valid_google_account: true)

      expect(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, _project_number, [body: _body] ->
          {:error,
           %Tesla.Env{
             status: 404,
             body: Jason.encode!(%{error: %{message: "User #{user.email} does not exist."}})
           }}
        end
      )

      assert ExUnit.CaptureLog.capture_log(fn ->
               CloudResourceManager.set_iam_policy(async: false)
             end) =~ "marked as invalid"

      user = Logflare.Repo.get!(Logflare.User, user.id)
      refute user.valid_google_account

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

      refute ExUnit.CaptureLog.capture_log(fn ->
               CloudResourceManager.set_iam_policy(async: false)
             end) =~ "marked as invalid"
    end

    test "sets team_user as invalid google account if user does not exist" do
      team_user = insert(:team_user, valid_google_account: true)

      expect(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, _project_number, [body: _body] ->
          {:error,
           %Tesla.Env{
             status: 404,
             body: Jason.encode!(%{error: %{message: "User #{team_user.email} does not exist."}})
           }}
        end
      )

      assert ExUnit.CaptureLog.capture_log(fn ->
               CloudResourceManager.set_iam_policy(async: false)
             end) =~ "marked as invalid"

      team_user = Logflare.Repo.get!(Logflare.TeamUsers.TeamUser, team_user.id)
      refute team_user.valid_google_account

      # attempting to re-set the policy should not raise an error
      refute ExUnit.CaptureLog.capture_log(fn ->
               CloudResourceManager.set_iam_policy(async: false)
             end) =~ "marked as invalid"
    end
  end

  defp setup_test_state() do
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
end
