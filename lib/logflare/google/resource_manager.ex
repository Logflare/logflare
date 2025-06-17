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
  alias Logflare.Utils.Tasks

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
    Tasks.start_child(fn -> set_iam_policy(async: false) end)
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
    for {member, roles} <- [
          {env_service_account(),
           [
             "roles/bigquery.admin",
             "roles/resourcemanager.projectIamAdmin",
             "roles/iam.serviceAccountCreator",
             "roles/iam.serviceAccountTokenCreator"
           ]},
          {env_compute_engine_sa(),
           [
             "roles/compute.instanceAdmin",
             "roles/artifactregistry.reader",
             "roles/artifactregistry.writer",
             "roles/logging.logWriter",
             "roles/monitoring.metricWriter"
           ]},
          {env_cloud_build_sa(),
           [
             "roles/cloudbuild.builds.builder",
             "roles/compute.admin",
             "roles/container.admin",
             "roles/cloudkms.cryptoKeyDecrypter",
             "roles/iam.serviceAccountUser",
             "roles/editor",
             "roles/cloudbuild.builds.editor",
             "roles/cloudbuild.serviceAgent"
           ]},
          {env_cloud_build_trigger_sa(),
           [
             "roles/cloudbuild.builds.editor",
             "roles/iam.serviceAccountUser",
             "roles/cloudbuild.serviceAgent"
           ]},
          {env_api_sa(), ["roles/editor", "roles/cloudbuild.builds.editor"]},
          {env_grafana_sa(), ["roles/bigquery.dataViewer", "roles/bigquery.jobUser"]}
        ],
        member,
        role <- roles do
      %Model.Binding{
        members: ["serviceAccount:" <> member],
        role: role
      }
    end
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

  defp env_cloud_build_trigger_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:cloud_build_trigger_sa]

  defp env_compute_engine_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:compute_engine_sa]

  defp env_grafana_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:grafana_sa]
end
