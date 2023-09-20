defmodule Logflare.Alerting do
  @moduledoc """
  The Alerting context.
  """

  import Ecto.Query, warn: false
  alias Logflare.Repo

  require Logger
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Adaptor.SlackAdaptor
  alias Logflare.Alerting.AlertQuery
  alias Logflare.User

  @doc """
  Returns the list of alert_queries.

  ## Examples

      iex> list_alert_queries()
      [%AlertQuery{}, ...]

  """
  def list_alert_queries(%User{id: user_id}) do
    from(q in AlertQuery, where: q.user_id == ^user_id)
    |> Repo.all()
  end

  @doc """
  Gets a single alert_query.

  Raises `Ecto.NoResultsError` if the Alert query does not exist.

  ## Examples

      iex> get_alert_query!(123)
      %AlertQuery{}

      iex> get_alert_query!(456)
      ** (Ecto.NoResultsError)

  """
  def get_alert_query!(id), do: Repo.get!(AlertQuery, id)

  def get_alert_query_by(kw) do
    Repo.get_by(AlertQuery, kw)
  end

  @doc """
  Creates a alert_query.

  ## Examples

      iex> create_alert_query(%{field: value})
      {:ok, %AlertQuery{}}

      iex> create_alert_query(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_alert_query(%User{} = user, attrs \\ %{}) do
    user
    |> Ecto.build_assoc(:alert_queries)
    |> Repo.preload(:user)
    |> AlertQuery.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a alert_query.

  ## Examples

      iex> update_alert_query(alert_query, %{field: new_value})
      {:ok, %AlertQuery{}}

      iex> update_alert_query(alert_query, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_alert_query(%AlertQuery{} = alert_query, attrs) do
    alert_query
    |> Repo.preload(:user)
    |> AlertQuery.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a alert_query.

  ## Examples

      iex> delete_alert_query(alert_query)
      {:ok, %AlertQuery{}}

      iex> delete_alert_query(alert_query)
      {:error, %Ecto.Changeset{}}

  """
  def delete_alert_query(%AlertQuery{} = alert_query) do
    Repo.delete(alert_query)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking alert_query changes.

  ## Examples

      iex> change_alert_query(alert_query)
      %Ecto.Changeset{data: %AlertQuery{}}

  """
  @spec change_alert_query(AlertQuery.t()) :: Ecto.Changeset.t()
  def change_alert_query(%AlertQuery{} = alert_query, attrs \\ %{}) do
    AlertQuery.changeset(alert_query, attrs)
  end

  @doc """
  Retrieves a Citrine.Job based on AlertQuery.
  Citrine.Job shares the same id as AlertQuery, resulting in a 1-1 relationship.
  """
  @spec get_alert_job(AlertQuery.t()) :: Citrine.Job.t()
  def get_alert_job(%AlertQuery{id: id}), do: get_alert_job(id)

  def get_alert_job(id) do
    case Logflare.AlertsScheduler.get_job(id) do
      {_pid, job} -> job
      nil -> nil
    end
  end

  @doc """
  Updates or creates a new Citrine.Job based on a given AlertQuery
  """
  @spec upsert_alert_job(AlertQuery.t()) :: {:ok, Citrine.Job.t()}
  def upsert_alert_job(%AlertQuery{} = alert_query) do
    Logflare.AlertsScheduler.put_job(%Citrine.Job{
      id: alert_query.id,
      schedule: alert_query.cron,
      extended_syntax: false,
      task: {:run_alert, [alert_query]}
    })

    {:ok, get_alert_job(alert_query)}
  end

  @doc """
  Initializes and ensures that all alert jobs are created.
  TODO: batching instead of loading whole table.
  """
  def init_alert_jobs do
    AlertQuery
    |> Repo.all()
    |> Stream.each(fn alert_query ->
      if get_alert_job(alert_query) == nil do
        upsert_alert_job(alert_query)
      end
    end)
    |> Stream.run()

    :ok
  end

  @doc """
  Performs the check lifecycle of an AlertQuery.

  Send notifications if necessary configurations are set. If no results are returned from the query execution, no alert is sent.
  """
  @spec run_alert(AlertQuery.t()) :: :ok
  def run_alert(%AlertQuery{} = alert_query) do
    alert_query = alert_query |> Repo.preload([:user])

    with {:ok, [_ | _] = results} <- execute_alert_query(alert_query) do
      if alert_query.webhook_notification_url do
        WebhookAdaptor.Client.send(alert_query.webhook_notification_url, %{
          "result" => results
        })
      end

      if alert_query.slack_hook_url do
        SlackAdaptor.send_message(alert_query.slack_hook_url, results)
      end

      :ok
    else
      {:ok, []} ->
        :ok

      other ->
        other
    end
  end

  @doc """
  Deletes an AlertQuery's Citrine.Job from the scheduler
  noop if already deleted.

  ### Examples
    iex> delete_alert_job(%AlertQuery{})
    :ok
    iex> delete_alert_job(alert_query.id)
    :ok
  """
  @spec delete_alert_job(AlertQuery.t() | number()) :: :ok
  def delete_alert_job(%AlertQuery{id: id}), do: delete_alert_job(id)

  def delete_alert_job(alert_id) do
    Logflare.AlertsScheduler.delete_job(alert_id)
  end

  @doc """
  Executes an AlertQuery and returns its results

  Requires `:user` key to be preloaded.

  ### Examples
    iex> execute_alert_query(alert_query)
    {:ok, [{"user_id" => "my-user-id"}]}
  """
  @spec execute_alert_query(AlertQuery.t()) :: {:ok, [map()]}
  def execute_alert_query(%AlertQuery{user: %User{}} = alert_query) do
    Logger.info("Executing AlertQuery | #{alert_query.name} | #{alert_query.id}")

    with {:ok, transformed_query} <-
           Logflare.Sql.transform(:bq_sql, alert_query.query, alert_query.user_id),
         {:ok, %{rows: rows}} <-
           Logflare.BqRepo.query_with_sql_and_params(
             alert_query.user,
             alert_query.user.bigquery_project_id || env_project_id(),
             transformed_query,
             [],
             parameterMode: "NAMED",
             maxResults: 1000,
             location: alert_query.user.bigquery_dataset_location
           ) do
      {:ok, rows}
    end
  end

  # helper to get the google project id via env.
  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]
end
