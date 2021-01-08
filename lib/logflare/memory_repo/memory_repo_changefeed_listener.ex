defmodule Logflare.Cache.InvalidationWorker do
  use Logflare.Commons
  use GenServer
  require Logger

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  @spec start_link([String.t()], [any]) :: {:ok, pid}
  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(channels) do
    results =
      for channel <- channels do
        Logger.debug("Starting #{__MODULE__} with channel subscription: #{channel}")
        {:ok, pid} = Postgrex.Notifications.start_link(Repo.config())
        {:ok, ref} = Postgrex.Notifications.listen(pid, channel)
        {pid, channel, ref}
      end

    {:ok, results}
  end

  @doc """
  Handle changefeed notification
  """
  def handle_info({:notification, _pid, _ref, _channel_name, payload}, _state) do
    payload
    |> Jason.decode!()
    |> invalidate_cache()

    {:noreply, :event_handled}
  catch
    huh, error ->
      Logger.error("Cache invalidation worker error: #{inspect(error)}")
      {:noreply, :event_error}
  end

  def handle_info(_value, _state) do
    {:noreply, :event_received}
  end

  def invalidate_cache(%{"type" => "DELETE"} = payload) do
    %{
      "id" => row_id,
      "old" => old_row_data,
      "table" => table,
      "type" => _type
    } = payload

    # delete
  end

  def invalidate_cache(%{"type" => type} = payload) when type in ["UPDATE", "INSERT"] do
    %{
      "id" => row_id,
      "new" => new_row_data,
      "old" => _old_row_data,
      "table" => table,
      "type" => _type
    } = payload

    # insert
  end

  def invalidate_cache(_payload) do
    :noop
  end

  defp table_to_schema("teams"), do: Team
  defp table_to_schema("team_users"), do: TeamUser
  defp table_to_schema("rules"), do: Rule
  defp table_to_schema("users"), do: User
  defp table_to_schema("sources"), do: Source
end
