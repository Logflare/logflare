defmodule Logflare.Google.CloudResourceManager do
  @moduledoc false
  require Logger

  import Ecto.Query

  alias GoogleApi.CloudResourceManager.V1.Api
  alias GoogleApi.CloudResourceManager.V1.Model
  alias Logflare.Repo
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.User
  alias Logflare.TeamUsers
  alias Logflare.Billing

  def list_projects() do
    conn = GenUtils.get_conn()
    Api.Projects.cloudresourcemanager_projects_list(conn)
  end

  def get_iam_policy() do
    conn = GenUtils.get_conn()

    body = %Model.GetIamPolicyRequest{}

    Api.Projects.cloudresourcemanager_projects_get_iam_policy(
      conn,
      env_project_number(),
      body: body
    )
  end

  def set_iam_policy(opts \\ [async: true])

  def set_iam_policy(async: true) do
    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn -> set_iam_policy(async: false) end)
  end

  def set_iam_policy(async: false) do
    conn = GenUtils.get_conn()
    members = build_members()

    bindings =
      [%Model.Binding{members: members, role: "roles/bigquery.jobUser"}] ++
        get_service_accounts()

    policy = %Model.Policy{bindings: bindings}
    body = %Model.SetIamPolicyRequest{policy: policy}

    {function, arity} = __ENV__.function
    caller = "#{function}" <> "_" <> "#{arity}"

    case Api.Projects.cloudresourcemanager_projects_set_iam_policy(conn, env_project_number(),
           body: body
         ) do
      {:ok, _response} ->
        Logger.info("Set IAM policy: #{Enum.count(members)} accounts",
          logflare: %{
            google: %{
              cloudresourcemanager: %{
                "#{caller}": %{
                  accounts: Enum.count(members),
                  response: :ok
                }
              }
            }
          }
        )

      {:error, response} ->
        Logger.error("Set IAM policy error: #{GenUtils.get_tesla_error_message(response)}",
          logflare: %{
            google: %{
              cloudresourcemanager: %{
                "#{caller}": %{
                  accounts: Enum.count(members),
                  response: :error,
                  response_message: "#{GenUtils.get_tesla_error_message(response)}"
                }
              }
            }
          }
        )
    end
  end

  defp get_service_accounts() do
    [
      %Model.Binding{
        members: ["serviceAccount:#{env_service_account()}"],
        role: "roles/bigquery.admin"
      },
      %Model.Binding{
        members: ["serviceAccount:#{env_service_account()}"],
        role: "roles/resourcemanager.projectIamAdmin"
      },
      %Model.Binding{
        members: ["serviceAccount:#{env_compute_engine_sa()}"],
        role: "roles/compute.instanceAdmin"
      },
      %Model.Binding{
        members: ["serviceAccount:#{env_compute_engine_sa()}"],
        role: "roles/containerregistry.ServiceAgent"
      },
      %Model.Binding{
        members: ["serviceAccount:#{env_compute_engine_sa()}"],
        role: "roles/logging.logWriter"
      },
      %Model.Binding{
        members: ["serviceAccount:#{env_compute_engine_sa()}"],
        role: "roles/monitoring.metricWriter"
      },
      %Model.Binding{
        condition: nil,
        members: ["serviceAccount:#{env_cloud_build_sa()}"],
        role: "roles/cloudbuild.builds.create"
      },
      %Model.Binding{
        condition: nil,
        members: ["serviceAccount:#{env_cloud_build_sa()}"],
        role: "roles/cloudbuild.builds.builder"
      },
      %Model.Binding{
        condition: nil,
        members: ["serviceAccount:#{env_cloud_build_sa()}"],
        role: "roles/compute.admin"
      },
      %Model.Binding{
        condition: nil,
        members: ["serviceAccount:#{env_cloud_build_sa()}"],
        role: "roles/container.admin"
      },
      %Model.Binding{
        condition: nil,
        members: ["serviceAccount:#{env_cloud_build_sa()}"],
        role: "roles/cloudkms.cryptoKeyDecrypter"
      },
      %Model.Binding{
        condition: nil,
        members: [
          "serviceAccount:#{env_cloud_build_sa()}"
        ],
        role: "roles/iam.serviceAccountUser"
      },
      %Model.Binding{
        members: ["serviceAccount:#{env_api_sa()}"],
        role: "roles/editor"
      }
    ]
  end

  defp build_members() do
    query =
      from(u in User,
        join: t in assoc(u, :team),
        preload: [team: t],
        select: u
      )

    all_paid_users =
      query
      |> Repo.all()
      |> Enum.filter(fn user ->
        case Billing.get_plan_by_user(user) do
          %Billing.Plan{name: "Free"} -> false
          _plan -> true
        end
      end)

    valid_paid_users =
      all_paid_users
      |> Enum.filter(&is_valid_member?/1)
      |> List.flatten()

    paid_users_team_members =
      all_paid_users
      |> Enum.map(fn paid_user ->
        team_users = TeamUsers.list_team_users_by(team_id: paid_user.team.id)
        Enum.filter(team_users, &is_valid_member?/1)
      end)
      |> List.flatten()

    (valid_paid_users ++ paid_users_team_members)
    |> Enum.sort_by(& &1.updated_at, {:desc, Date})
    |> Enum.take(1450)
    |> Enum.map(&("user:" <> &1.email))
  end

  defp is_valid_member?(%{provider: "google", valid_google_account: true}), do: true
  defp is_valid_member?(%{provider: "google", valid_google_account: nil}), do: true
  defp is_valid_member?(_), do: false

  defp env_project_number, do: Application.get_env(:logflare, Logflare.Google)[:project_number]
  defp env_service_account, do: Application.get_env(:logflare, Logflare.Google)[:service_account]
  defp env_api_sa, do: Application.get_env(:logflare, Logflare.Google)[:api_sa]
  defp env_cloud_build_sa, do: Application.get_env(:logflare, Logflare.Google)[:cloud_build_sa]

  defp env_compute_engine_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:compute_engine_sa]
end
