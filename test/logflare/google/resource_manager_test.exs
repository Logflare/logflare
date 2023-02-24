defmodule Logflare.Google.CloudResourceManagerTest do
  use Logflare.DataCase, async: false
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
    test "request body sends expected roles for paying users, paying team users and service accounts",
         %{
           google_configs: google_configs,
           expected_members: expected_members
         } do
      stub(
        GoogleApi.CloudResourceManager.V1.Api.Projects,
        :cloudresourcemanager_projects_set_iam_policy,
        fn _, project_number, [body: body] ->
          assert project_number == google_configs.project_number

          assert {[%{members: members, role: role}], service_accounts} =
                   Enum.split_with(body.policy.bindings, fn %{role: role} ->
                     role == "roles/bigquery.jobUser"
                   end)

          assert members == expected_members
          assert role == "roles/bigquery.jobUser"
          assert service_accounts == expected_service_accounts(google_configs)
          {:ok, ""}
        end
      )

      CloudResourceManager.set_iam_policy(async: false)
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

  defp expected_service_accounts(google_configs) do
    [
      %Binding{
        members: ["serviceAccount:#{google_configs.service_account}"],
        role: "roles/bigquery.admin"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.service_account}"],
        role: "roles/resourcemanager.projectIamAdmin"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.compute_engine_sa}"],
        role: "roles/compute.instanceAdmin"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.compute_engine_sa}"],
        role: "roles/containerregistry.ServiceAgent"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.compute_engine_sa}"],
        role: "roles/logging.logWriter"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.compute_engine_sa}"],
        role: "roles/monitoring.metricWriter"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.cloud_build_sa}"],
        role: "roles/cloudbuild.builds.builder"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.cloud_build_sa}"],
        role: "roles/compute.admin"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.cloud_build_sa}"],
        role: "roles/container.admin"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.cloud_build_sa}"],
        role: "roles/cloudkms.cryptoKeyDecrypter"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.cloud_build_sa}"],
        role: "roles/iam.serviceAccountUser"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.api_sa}"],
        role: "roles/editor"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.api_sa}"],
        role: "roles/cloudbuild.builds.editor"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.cloud_build_trigger_sa}"],
        role: "roles/cloudbuild.builds.editor"
      },
      %Binding{
        members: ["serviceAccount:#{google_configs.cloud_build_trigger_sa}"],
        role: "roles/iam.serviceAccountUser"
      }
    ]
  end
end
