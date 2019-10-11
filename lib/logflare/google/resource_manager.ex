defmodule Logflare.Google.CloudResourceManager do
  @moduledoc false
  require Logger

  import Ecto.Query, only: [from: 2]

  alias GoogleApi.CloudResourceManager.V1.Api
  alias GoogleApi.CloudResourceManager.V1.Model
  alias GoogleApi.CloudResourceManager.V1.Connection
  alias Logflare.Repo
  alias Logflare.Google.BigQuery.GenUtils

  @project_number Application.get_env(:logflare, Logflare.Google)[:project_number]
  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account]
  @api_sa Application.get_env(:logflare, Logflare.Google)[:api_sa]
  @compute_engine_sa Application.get_env(:logflare, Logflare.Google)[:compute_engine_sa]

  def get_iam_policy() do
    conn = get_conn()

    body = %Model.GetIamPolicyRequest{}

    Api.Projects.cloudresourcemanager_projects_get_iam_policy(conn, @project_number, body: body)
  end

  def set_iam_policy() do
    conn = get_conn()

    Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
      members = build_members()

      bindings =
        [
          %Model.Binding{
            members: members,
            role: "roles/bigquery.jobUser"
          }
        ] ++ get_service_accounts()

      policy = %Model.Policy{
        bindings: bindings
      }

      body = %Model.SetIamPolicyRequest{
        policy: policy
      }

      {f, a} = __ENV__.function
      fun = "#{f}" <> "_" <> "#{a}"

      case Api.Projects.cloudresourcemanager_projects_set_iam_policy(conn, @project_number,
             body: body
           ) do
        {:ok, _response} ->
          Logger.info("Set IAM policy: #{Enum.count(members)} accounts",
            logflare: %{
              google: %{
                cloudresourcemanager: %{
                  "#{fun}": %{accounts: Enum.count(members), response: :ok}
                }
              }
            }
          )

        {:error, response} ->
          Logger.error("Set IAM policy error: #{GenUtils.get_tesla_error_message(response)}",
            logflare: %{
              google: %{
                cloudresourcemanager: %{
                  "#{fun}": %{
                    accounts: Enum.count(members),
                    response: :error,
                    response_message: "#{GenUtils.get_tesla_error_message(response)}"
                  }
                }
              }
            }
          )
      end
    end)
  end

  def list_projects() do
    conn = get_conn()
    Api.Projects.cloudresourcemanager_projects_list(conn)
  end

  defp get_service_accounts() do
    [
      %Model.Binding{
        members: ["serviceAccount:#{@service_account}"],
        role: "roles/bigquery.admin"
      },
      %Model.Binding{
        members: ["serviceAccount:#{@compute_engine_sa}"],
        role: "roles/editor"
      },
      %Model.Binding{
        members: ["serviceAccount:#{@api_sa}"],
        role: "roles/editor"
      },
      %Model.Binding{
        members: ["serviceAccount:#{@service_account}"],
        role: "roles/resourcemanager.projectIamAdmin"
      }
    ]
  end

  defp build_members() do
    query =
      from(u in "users",
        where: u.provider == "google",
        where: u.valid_google_account == true or is_nil(u.valid_google_account),
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
