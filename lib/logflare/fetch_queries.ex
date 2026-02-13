defmodule Logflare.FetchQueries do
  @moduledoc """
  The FetchQueries context for pull-based data ingestion.
  """

  import Ecto.Query, warn: false

  alias Logflare.FetchQueries.FetchQuery
  alias Logflare.Backends
  alias Logflare.Repo
  alias Logflare.Teams
  alias Logflare.User

  require Logger

  @doc """
  Returns the list of fetch queries a user has access to, including where the user is a team member.
  """
  @spec list_fetch_queries_by_user_access(User.t()) :: [FetchQuery.t()]
  def list_fetch_queries_by_user_access(%User{} = user) do
    FetchQuery
    |> Teams.filter_by_user_access(user)
    |> Repo.all()
    |> Enum.map(&preload_fetch_query/1)
  end

  @doc """
  Gets a single fetch query.
  """
  def get_fetch_query(id) when is_integer(id) do
    Repo.get(FetchQuery, id)
  end

  @doc """
  Gets a fetch query by external_id.
  """
  def get_fetch_query_by_external_id(external_id) do
    Repo.get_by(FetchQuery, external_id: external_id)
  end

  @doc """
  Gets a fetch query by id that the user has access to.
  Returns the fetch query if the user owns it or is a team member, otherwise returns nil.
  """
  @spec get_fetch_query_by_user_access(User.t(), integer() | String.t()) :: FetchQuery.t() | nil
  def get_fetch_query_by_user_access(user_or_team_user, id)
      when is_integer(id) or is_binary(id) do
    FetchQuery
    |> Teams.filter_by_user_access(user_or_team_user)
    |> where([fq], fq.id == ^id)
    |> Repo.one()
  end

  @doc """
  Preload fetch query with backend and source.
  """
  def preload_fetch_query(fetch_query) do
    fetch_query
    |> Repo.preload([:user, :backend, :source])
    |> then(fn %FetchQuery{backend: backend} = fq ->
      case backend do
        nil ->
          fq

        %{} ->
          %{fq | backend: Backends.typecast_config_string_map_to_atom_map(backend)}
      end
    end)
  end

  @doc """
  Gets enabled fetch queries (for scheduler).
  """
  def list_enabled_fetch_queries do
    FetchQuery
    |> where([fq], fq.enabled == true)
    |> Repo.all()
  end

  @doc """
  Creates a fetch query.
  """
  def create_fetch_query(attrs \\ %{}) do
    attrs = resolve_backend_id(attrs)

    %FetchQuery{}
    |> FetchQuery.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a fetch query.
  """
  def update_fetch_query(%FetchQuery{} = fetch_query, attrs) do
    attrs = resolve_backend_id(attrs)

    fetch_query
    |> FetchQuery.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a fetch query.
  """
  def delete_fetch_query(%FetchQuery{} = fetch_query) do
    Repo.delete(fetch_query)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking fetch_query changes.
  """
  def change_fetch_query(%FetchQuery{} = fetch_query, attrs \\ %{}) do
    FetchQuery.changeset(fetch_query, attrs)
  end

  @doc """
  Lists execution history from Oban jobs for a fetch query.
  Returns up to 50 most recent jobs.
  """
  def list_execution_history(fetch_query_id) when is_integer(fetch_query_id) do
    from(job in Oban.Job,
      where:
        fragment(
          "?->>'fetch_query_id' = ?",
          job.args,
          ^to_string(fetch_query_id)
        ),
      order_by: [desc: job.scheduled_at],
      limit: 50
    )
    |> Repo.all()
  end

  @doc """
  Syncs fetch query schedule by canceling old jobs and creating new ones.
  """
  def sync_fetch_query_schedule(%FetchQuery{id: id}) do
    # Cancel existing jobs for this fetch query
    from(job in Oban.Job,
      where:
        fragment(
          "?->>'fetch_query_id' = ?",
          job.args,
          ^to_string(id)
        ) and job.state not in ["completed", "discarded", "cancelled"]
    )
    |> Repo.all()
    |> Enum.each(&Oban.cancel_job/1)

    :ok
  end

  @doc """
  Triggers a fetch query to run immediately by creating an Oban job with schedule_in: 0.
  """
  @spec trigger_fetch_query_now(integer()) :: {:ok, Oban.Job.t()} | {:error, term()}
  def trigger_fetch_query_now(fetch_query_id) when is_integer(fetch_query_id) do
    %{
      fetch_query_id: fetch_query_id,
      scheduled_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }
    |> Logflare.FetchQueries.FetchQueryWorker.new(schedule_in: 0)
    |> Oban.insert()
  end

  @doc """
  Partitions jobs into future (scheduled/available/executing) and past (completed/discarded/cancelled).
  """
  @spec partition_jobs_by_time([Oban.Job.t()]) :: {[Oban.Job.t()], [Oban.Job.t()]}
  def partition_jobs_by_time(jobs) do
    Enum.split_with(jobs, fn job ->
      job.state in ["available", "scheduled", "executing"]
    end)
  end

  defp resolve_backend_id(attrs) do
    # Check if backend_id is nil or empty
    backend_id = Map.get(attrs, "backend_id") || Map.get(attrs, :backend_id)

    case backend_id do
      nil ->
        maybe_set_default_backend(attrs)

      "" ->
        maybe_set_default_backend(attrs)

      _ ->
        attrs
    end
  end

  defp maybe_set_default_backend(attrs) do
    user_id = Map.get(attrs, "user_id") || Map.get(attrs, :user_id)

    case user_id do
      nil ->
        attrs

      user_id ->
        case Repo.get(User, user_id) do
          nil ->
            attrs

          user ->
            default_backend = Backends.get_default_backend(user)

            # Determine which key type to use based on attrs structure
            key =
              if Map.has_key?(attrs, "backend_id") or Enum.all?(Map.keys(attrs), &is_binary/1) do
                "backend_id"
              else
                :backend_id
              end

            Map.put(attrs, key, default_backend.id)
        end
    end
  end
end
