defmodule Logflare.Google.CloudResourceManager do
  @moduledoc false

  import Ecto.Query

  require Logger

  alias GoogleApi.CloudResourceManager.V1.Api
  alias GoogleApi.CloudResourceManager.V1.Model
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Repo
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Utils.Tasks

  @user_role "roles/bigquery.jobUser"

  def list_projects do
    conn = GenUtils.get_conn()
    Api.Projects.cloudresourcemanager_projects_list(conn)
  end

  def get_iam_policy do
    conn = GenUtils.get_conn()

    body = %Model.GetIamPolicyRequest{
      options: %Model.GetPolicyOptions{requestedPolicyVersion: 3}
    }

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

  @spec append_managed_sa_to_iam_policy(User.t()) ::
          {:ok, GoogleApi.CloudResourceManager.V1.Model.Policy.t()}
          | {:error, :no_project_id}
          | {:error, :managed_service_accounts_disabled}
          | {:error, term()}
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

  @spec append_managed_sa_to_all_iam_projects(User.t()) :: %{
          primary: {:ok, Model.Policy.t()} | {:error, term()},
          additional: [
            {String.t(), {:ok, Model.Policy.t()} | {:error, term()}}
          ]
        }
  def append_managed_sa_to_all_iam_projects(user) do
    primary_result = append_managed_sa_to_iam_policy(user)

    additional_results =
      user
      |> User.parse_bigquery_additional_projects()
      |> Enum.map(fn project_id ->
        {project_id, append_managed_sa_to_additional_project(user, project_id)}
      end)

    %{primary: primary_result, additional: additional_results}
  end

  defp append_managed_sa_to_additional_project(
         %User{bigquery_enable_managed_service_accounts: false},
         _project_id
       ),
       do: {:error, :managed_service_accounts_disabled}

  defp append_managed_sa_to_additional_project(_user, project_id) do
    with {:enabled?, true} <- {:enabled?, BigQueryAdaptor.managed_service_accounts_enabled?()},
         {:ok, policy} <- get_iam_policy_for_project(project_id),
         {:contains?, _policy, false} <-
           {:contains?, policy, contains_managed_service_accounts?(policy)} do
      append_managed_service_accounts(project_id, policy)
    else
      {:contains?, policy, _} -> {:ok, policy}
      {:enabled?, false} -> {:error, :managed_service_accounts_disabled}
      {:error, _err} = err -> err
    end
  end

  defp get_iam_policy_for_project(project_id) do
    conn = GenUtils.get_conn()

    Api.Projects.cloudresourcemanager_projects_get_iam_policy(
      conn,
      project_id,
      body: %Model.GetIamPolicyRequest{}
    )
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

  @spec set_iam_policy(keyword()) :: :ok | {:error, term()} | term()
  def set_iam_policy(opts \\ [async: true])

  def set_iam_policy(async: true) do
    Tasks.start_child(fn -> set_iam_policy(async: false) end)
  end

  def set_iam_policy(async: false) do
    project = env_project_number()

    with {:ok, policy} <- get_iam_policy(),
         {:ok, policy} <-
           append_bindings(
             project,
             policy,
             get_service_accounts() ++ build_project_viewer_bindings()
           ) do
      {added, failed} = append_user_members(project, policy, build_members())

      :telemetry.execute(
        [:logflare, :google, :set_iam_policy],
        %{added: added, failed: failed},
        %{}
      )

      if failed == 0 do
        Logger.info("Appended IAM policy for #{project} successful, added #{added} user members")
      else
        Logger.error(
          "Appended IAM policy for #{project} with #{failed} failed user members, added #{added}"
        )
      end

      :ok
    else
      {:error, err} = error ->
        Logger.error("Append IAM policy for #{project} failed: #{format_error(err)}")
        error
    end
  end

  @spec append_bindings(String.t() | integer(), Model.Policy.t(), [Model.Binding.t()]) ::
          {:ok, Model.Policy.t()} | {:error, term()}
  defp append_bindings(project, %Model.Policy{} = policy, bindings) do
    merged = merge_bindings(policy, bindings)

    if merged == policy do
      {:ok, policy}
    else
      write_policy(project, merged)
    end
  end

  @spec merge_bindings(Model.Policy.t(), [Model.Binding.t()]) :: Model.Policy.t()
  defp merge_bindings(%Model.Policy{} = policy, new_bindings) do
    Enum.reduce(new_bindings, policy, fn %Model.Binding{role: role, members: members}, acc ->
      existing = acc.bindings || []
      idx = Enum.find_index(existing, &(&1.role == role and is_nil(&1.condition)))
      %{acc | bindings: merge_binding(existing, idx, role, members)}
    end)
  end

  defp merge_binding(existing, nil, role, members),
    do: existing ++ [%Model.Binding{role: role, members: members}]

  defp merge_binding(existing, idx, _role, members) do
    List.update_at(existing, idx, fn binding ->
      current = binding.members || []
      %{binding | members: current ++ Enum.reject(members, &(&1 in current))}
    end)
  end

  @spec append_user_members(String.t() | integer(), Model.Policy.t(), [String.t()]) ::
          {non_neg_integer(), non_neg_integer()}
  defp append_user_members(project, policy, members) do
    current =
      policy.bindings
      |> List.wrap()
      |> Enum.filter(&(&1.role == @user_role and is_nil(&1.condition)))
      |> Enum.flat_map(&(&1.members || []))

    {_policy, added, failed} =
      members
      |> Enum.reject(&(&1 in current))
      |> Enum.reduce({policy, 0, 0}, fn member, {acc, added, failed} ->
        binding = %Model.Binding{role: @user_role, members: [member]}

        case write_policy(project, merge_bindings(acc, [binding])) do
          {:ok, new_policy} ->
            {new_policy, added + 1, failed}

          {:error, err} ->
            handle_member_error(member, err)
            {refresh_policy(acc), added, failed + 1}
        end
      end)

    {added, failed}
  end

  defp refresh_policy(fallback) do
    case get_iam_policy() do
      {:ok, policy} -> policy
      {:error, _} -> fallback
    end
  end

  defp write_policy(project, %Model.Policy{} = policy) do
    body = %Model.SetIamPolicyRequest{policy: %{policy | version: 3}}
    conn = GenUtils.get_conn()

    case Api.Projects.cloudresourcemanager_projects_set_iam_policy(conn, project, body: body) do
      {:ok, %Model.Policy{} = new_policy} -> {:ok, new_policy}
      {:ok, _} -> {:ok, policy}
      {:error, _} = err -> err
    end
  end

  @spec handle_member_error(String.t(), term()) :: :ok
  defp handle_member_error("user:" <> email = member, %Tesla.Env{} = response) do
    message = GenUtils.get_tesla_error_message(response)

    if message =~ ~r/User \S+ does not exist/ or message =~ ~r/is of type "\w+"/ do
      count = invalidate_google_account(email)

      Logger.warning(
        "IAM member #{member} rejected, marked #{count} account(s) as invalid google account: #{message}"
      )
    else
      Logger.error("Adding IAM member #{member} failed: #{message}")
    end

    :ok
  end

  defp handle_member_error(member, err) do
    Logger.error("Adding IAM member #{member} failed: #{format_error(err)}")
    :ok
  end

  @spec invalidate_google_account(String.t()) :: non_neg_integer()
  defp invalidate_google_account(email) do
    {users, _} =
      from(u in User, where: u.email == ^email)
      |> Repo.update_all(set: [valid_google_account: false])

    {team_users, _} =
      from(tu in TeamUser, where: tu.email == ^email)
      |> Repo.update_all(set: [valid_google_account: false])

    users + team_users
  end

  defp format_error(%Tesla.Env{} = response), do: GenUtils.get_tesla_error_message(response)
  defp format_error(err), do: inspect(err)

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
            {GCPConfig.service_account(),
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
            {env_grafana_sa(), ["roles/bigquery.dataViewer", "roles/bigquery.jobUser"]},
            {env_cloud_sql_client_sa(), ["roles/cloudsql.client"]}
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
    |> Enum.uniq()
    |> Enum.take(1400)
    |> Enum.map(&("user:" <> &1))
  end

  defp env_project_number, do: Application.get_env(:logflare, Logflare.Google)[:project_number]
  defp env_api_sa, do: Application.get_env(:logflare, Logflare.Google)[:api_sa]
  defp env_cloud_build_sa, do: Application.get_env(:logflare, Logflare.Google)[:cloud_build_sa]

  defp env_cloud_build_trigger_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:cloud_build_trigger_sa]

  defp env_compute_engine_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:compute_engine_sa]

  defp env_grafana_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:grafana_sa]

  defp env_cloud_sql_client_sa,
    do: Application.get_env(:logflare, Logflare.Google)[:cloud_sql_client_sa]

  defp env_project_viewer,
    do: Application.get_env(:logflare, Logflare.Google)[:project_viewer]

  defp build_project_viewer_bindings do
    case env_project_viewer() do
      nil ->
        []

      value ->
        member =
          if String.starts_with?(value, "user:") or String.starts_with?(value, "group:") do
            value
          else
            "user:" <> value
          end

        [%Model.Binding{members: [member], role: "roles/viewer"}]
    end
  end
end
