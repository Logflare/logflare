defmodule Logflare.Google.CloudResourceManager do
  @moduledoc false

  alias GoogleApi.CloudResourceManager.V1.Api
  alias GoogleApi.CloudResourceManager.V1.Model
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Users
  alias Logflare.User
  alias Logflare.TeamUsers
  alias Logflare.Utils.Tasks
  alias Logflare.Backends.Adaptor.BigQueryAdaptor

  require Logger

  def list_projects do
    conn = GenUtils.get_conn()
    Api.Projects.cloudresourcemanager_projects_list(conn)
  end

  def get_iam_policy do
    conn = GenUtils.get_conn()

    body = %Model.GetIamPolicyRequest{}

    Api.Projects.cloudresourcemanager_projects_get_iam_policy(
      conn,
      env_project_number(),
      body: body
    )
  end

  def get_iam_policy(%User{bigquery_project_id: nil}), do: {:error, :no_project_id}

  def get_iam_policy(user) do
    conn = GenUtils.get_conn()

    Api.Projects.cloudresourcemanager_projects_get_iam_policy(
      conn,
      user.bigquery_project_id,
      body: %Model.GetIamPolicyRequest{}
    )
  end

  def append_managed_sa_to_iam_policy(%User{bigquery_project_id: nil}),
    do: {:error, :no_project_id}

  def append_managed_sa_to_iam_policy(%User{bigquery_enable_managed_service_accounts: false}),
    do: {:error, :managed_service_accounts_disabled}

  def append_managed_sa_to_iam_policy(user) do
    with {:enabled?, true} <- {:enabled?, BigQueryAdaptor.managed_service_accounts_enabled?()},
         {:ok, policy} <- get_iam_policy(user),
         {:contains?, _policy, false} <-
           {:contains?, policy, contains_managed_service_accounts?(policy)} do
      append_managed_service_accounts(user.bigquery_project_id, policy)
    else
      {:contains?, policy, _} -> {:ok, policy}
      {:enabled?, false} -> {:error, :managed_service_accounts_disabled}
      {:error, _err} = err -> err
    end
  end

  # returns false if missing any of the managed service accounts
  defp contains_managed_service_accounts?(%Model.Policy{bindings: bindings}) do
    ids = BigQueryAdaptor.managed_service_account_ids()
    members = Enum.flat_map(bindings, fn %{members: members} -> members end)

    Enum.all?(ids, fn id ->
      Enum.any?(members, fn member ->
        member =~ id
      end)
    end)
  end

  def append_managed_service_accounts(project_id, %Model.Policy{bindings: bindings})
      when project_id != nil do
    conn = GenUtils.get_conn()

    members =
      for %{email: name} <- BigQueryAdaptor.list_managed_service_accounts() do
        "serviceAccount:" <> name
      end

    new_binding = %Model.Binding{members: members, role: "roles/bigquery.admin"}

    policy = %Model.Policy{bindings: [new_binding | bindings]}
    body = %Model.SetIamPolicyRequest{policy: policy}

    case Api.Projects.cloudresourcemanager_projects_set_iam_policy(conn, project_id, body: body) do
      {:ok, response} ->
        Logger.info(
          "Appended managed service accounts to IAM policy for #{project_id} successful"
        )

        {:ok, response}

      {:error, err} ->
        Logger.error(
          "Append managed service accounts to IAM policy for #{project_id} unknown error: #{inspect(err)}"
        )

        {:error, err}
    end
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

    case Api.Projects.cloudresourcemanager_projects_set_iam_policy(conn, env_project_number(),
           body: body
         ) do
      {:ok, _response} ->
        :telemetry.execute(
          [:logflare, :google, :set_iam_policy],
          %{members: Enum.uniq(members) |> Enum.count()},
          %{}
        )

        Logger.info("Set IAM policy successful")

      {:error, _err} = err ->
        handle_errors(err)
    end
  end

  defp handle_errors({:error, %Tesla.Env{} = response}) do
    message = GenUtils.get_tesla_error_message(response)
    user_exists_regexp = ~r/User (\S+?@\S+) does not exist/

    if message =~ user_exists_regexp do
      [captured] = Regex.run(user_exists_regexp, message, capture: :all_but_first)
      # set user as invalid google account
      result =
        cond do
          user = Users.get_by(email: captured) ->
            user
            |> Users.update_user_all_fields(%{valid_google_account: false})

          team_user = TeamUsers.get_team_user_by(email: captured) ->
            team_user
            |> TeamUsers.update_team_user(%{valid_google_account: false})

          true ->
            :noop
        end

      if result == :noop do
        Logger.error(
          "Could find user #{captured} in the database. Set IAM policy error: #{message}",
          error_string: Jason.decode!(response.body)
        )
      else
        Logger.info(
          "Google account #{captured} was marked as invalid and excluded from IAM policy",
          error_string: Jason.decode!(response.body)
        )
      end
    else
      Logger.error("Set IAM policy unknown API error: #{message}",
        error_string: Jason.decode!(response.body)
      )

      :noop
    end
  end

  defp handle_errors({:error, err}) do
    Logger.error("Set IAM policy unknown error: #{inspect(err)}")
  end

  defp get_service_accounts do
    managed_service_accounts =
      if BigQueryAdaptor.managed_service_accounts_enabled?() do
        for %{email: name} <- BigQueryAdaptor.list_managed_service_accounts() do
          {name, ["roles/bigquery.admin"]}
        end
      else
        []
      end

    for {member, roles} <-
          [
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
          ] ++ managed_service_accounts,
        member,
        role <- roles do
      %Model.Binding{
        members: ["serviceAccount:" <> member],
        role: role
      }
    end
  end

  defp build_members do
    emails =
      Users.list_users(paying: true, provider: :google)
      |> Users.preload_valid_google_team_users()
      |> Enum.flat_map(fn user ->
        [
          user.email
          | for tu <- user.team.team_users do
              tu.email
            end
        ]
      end)

    if length(emails) > 1000 do
      Logger.warning(
        "Number of user emails attached to IAM policy is greater than 1000 (current: #{length(emails)}), taking first 1400"
      )
    end

    emails
    |> Enum.take(1400)
    |> Enum.map(&("user:" <> &1))
  end

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
