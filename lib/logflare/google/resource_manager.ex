defmodule Logflare.Google.CloudResourceManager do
  require Logger

  import Ecto.Query, only: [from: 2]

  alias GoogleApi.CloudResourceManager.V1.Api
  alias GoogleApi.CloudResourceManager.V1.Model
  alias GoogleApi.CloudResourceManager.V1.Connection
  alias Logflare.Repo

  @project_number Application.get_env(:logflare, Logflare.Google)[:project_number]
  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account]

  def get_iam_policy() do
    conn = get_conn()

    body = %Model.GetIamPolicyRequest{}

    Api.Projects.cloudresourcemanager_projects_get_iam_policy(conn, @project_number, body: body)
  end

  def set_iam_policy() do
    conn = get_conn()

    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
      members = build_members()

      bindings = [
        %Model.Binding{
          members: ["serviceAccount:#{@service_account}"],
          role: "roles/bigquery.admin"
        },
        %Model.Binding{
          members: members,
          role: "roles/bigquery.jobUser"
        },
        %Model.Binding{
          members: ["user:chase@logflare.app"],
          role: "roles/owner"
        },
        %Model.Binding{
          members: ["serviceAccount:#{@service_account}"],
          role: "roles/resourcemanager.projectIamAdmin"
        }
      ]

      policy = %Model.Policy{
        bindings: bindings
      }

      body = %Model.SetIamPolicyRequest{
        policy: policy
      }

      Api.Projects.cloudresourcemanager_projects_set_iam_policy(conn, @project_number, body: body)

      Logger.info("IAM policy set: #{Enum.count(members)} accounts")
    end)
  end

  def list_projects() do
    conn = get_conn()
    Api.Projects.cloudresourcemanager_projects_list(conn)
  end

  defp build_members() do
    query =
      from(u in "users",
        where: u.provider == "google",
        select: %{
          email: u.email
        }
      )

    emails = Repo.all(query)

    Enum.map(emails, fn e ->
      "user:" <> e.email
    end)
  end

  defp get_conn() do
    {:ok, token} = Goth.Token.for_scope("https://www.googleapis.com/auth/cloud-platform")
    Connection.new(token.token)
  end
end
