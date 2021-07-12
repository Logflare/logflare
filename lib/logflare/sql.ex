defmodule Logflare.SQL do
  @moduledoc """
  SQL transformer functionality
  """
  require Logger

  # Build logflare_sql
  Mix.Tasks.Sql.run(nil)

  use GenServer, id: __MODULE__

  defstruct pid: nil, ready: false, ready_queue: [], requests: %{}

  def start_link(arg) do
    GenServer.start_link(__MODULE__, [arg], name: __MODULE__)
  end

  def init(_) do
    # Start networking if it isn't already
    unless Node.alive? do
      {:ok, _} = Node.start(:logflare)
    end
    {:ok, %__MODULE__{}, {:continue, :run}}
  end

  def handle_continue(:run, state) do
    db_config = Logflare.Repo.config
    db_url = "jdbc:pgsql://#{db_config[:hostname]}/#{db_config[:database]}"
    :exec.run_link(:code.priv_dir(:logflare) |>
                   to_string() |>
                   Path.join("sql/bin/logflare_sql"),
                   [
                     {:stdout, fn _, _, b -> IO.puts "[SQL] #{b}" end},
                     {:stderr, fn _, _, b -> IO.puts "[SQL(error)] #{b}" end},
                     {:env, [
                       {"NODE_NAME", node() |> to_string()},
                       {"COOKIE", Node.get_cookie |> to_string()},
                       {"DATABASE_URL", db_url},
                       {"DB_USER", db_config[:username]},
                       {"DB_PASSWORD", db_config[:password]},
                       {"PROJECT_ID", Application.get_env(:logflare, Logflare.Google)[:project_id]}
                     ]}
                   ])
    {:noreply, state}
  end

  def handle_call({:transform, query, user_id}, from, %__MODULE__{ready: false} = state) do
    {:noreply, %{state | ready_queue: [{{:transform, query, user_id}, from}|state.ready_queue]}}
  end

  def handle_call({:parameters, query}, from, %__MODULE__{ready: false} = state) do
    {:noreply, %{state | ready_queue: [{{:parameters, query}, from}|state.ready_queue]}}
  end

  def handle_call({:sources, query, user_id}, from, %__MODULE__{ready: false} = state) do
    {:noreply, %{state | ready_queue: [{{:sources, query, user_id}, from}|state.ready_queue]}}
  end

  def handle_call({:transform, query, user_id}, from, state) do
    ref = make_ref()
    send(state.pid, {:transform, self(), ref, query, user_id})
    {:noreply, put_in(state.requests[ref], from)}
  end

  def handle_call({:parameters, query}, from, state) do
    ref = make_ref()
    send(state.pid, {:parameters, self(), ref, query})
    {:noreply, put_in(state.requests[ref], from)}
  end

  def handle_call({:sources, query, user_id}, from, state) do
    ref = make_ref()
    send(state.pid, {:sources, self(), ref, query, user_id})
    {:noreply, put_in(state.requests[ref], from)}
  end

  def handle_info({:ready, pid}, %__MODULE__{} = state) do
    Logger.info("Logflare.SQL is ready")
    state = Enum.reduce(state.ready_queue, %__MODULE__ { state | pid: pid, ready_queue: [], ready: true },
    fn {call, from}, state ->
      {:noreply, state} = handle_call(call, from, state)
      state
    end)
    {:noreply, state}
  end

  def handle_info({:ok, ref, response}, state) do
    GenServer.reply(state.requests[ref], {:ok, response})
    {:noreply, pop_in(state.requests[ref]) |> elem(1) }
  end

  def handle_info({:error, ref, response}, state) do
    GenServer.reply(state.requests[ref], {:error, response})
    {:noreply, pop_in(state.requests[ref]) |> elem(1) }
  end

  @doc """
  Transform a query for a given User or User ID
  """
  def transform(query, user, timeout \\ 60_000) do
    do_transform(query, user, timeout)
  end

  defp do_transform(query, %Logflare.User{id: id}, timeout) do
    do_transform(query, id, timeout)
  end

  defp do_transform(query, user_id, timeout) do
    GenServer.call(__MODULE__, {:transform, query, user_id}, timeout)
  end

  @doc """
  Gets parameters from the query
  """
  def parameters(query, timeout \\ 60_000) do
    GenServer.call(__MODULE__, {:parameters, query}, timeout)
  end

  @doc """
  Get sources UUIDs from the query
  """
  def sources(query, user, timeout \\ 60_000) do
    do_sources(query, user, timeout)
  end

  defp do_sources(query, %Logflare.User{id: id}, timeout) do
    do_sources(query, id, timeout)
  end

  defp do_sources(query, user_id, timeout) do
    GenServer.call(__MODULE__, {:sources, query, user_id}, timeout)
  end


end