defmodule Logflare.ContextCache.WriteFence do
  @moduledoc """
  Orders cache population against cache invalidations.

  Cache reads and warmers open a snapshot before reading the database and commit
  entries grouped by the primary keys they depend on. Invalidations recorded
  while that snapshot is active cause only affected groups to be skipped. Cache
  writes and invalidation recording are serialized by this process, so an
  invalidation either prevents a stale write or runs before the existing
  cache-busting pass removes it.
  """

  use GenServer

  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBuster

  @active_table Module.concat(__MODULE__, ActiveContexts)

  @type snapshot :: reference()
  @type cache_entry :: {any(), any()}
  @type dependency :: any() | {:any, [any()]}
  @type entry_group :: {dependency(), [cache_entry()]}
  @type commit_result :: %{written: non_neg_integer(), skipped: non_neg_integer()}

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec begin_snapshot(module(), GenServer.server()) :: {:ok, snapshot()}
  def begin_snapshot(context, server \\ __MODULE__) do
    GenServer.call(server, {:begin_snapshot, context, self()}, :infinity)
  end

  @spec mark_ready(GenServer.server()) :: :ok
  def mark_ready(server \\ __MODULE__) do
    GenServer.call(server, {:mark_ready, self()})
  end

  @spec invalidate([{module(), any()}], GenServer.server()) :: :ok
  def invalidate(values, server \\ __MODULE__)

  def invalidate([], _server), do: :ok

  def invalidate(values, __MODULE__) do
    invalidations = normalize_invalidations(values)

    if active_context?(invalidations) do
      try do
        GenServer.call(__MODULE__, {:invalidate, invalidations}, :infinity)
      catch
        :exit, _reason -> :ok
      end
    else
      :ok
    end
  end

  def invalidate(values, server) do
    GenServer.call(server, {:invalidate, normalize_invalidations(values)}, :infinity)
  end

  @spec commit(snapshot(), module(), [entry_group()], GenServer.server()) ::
          {:ok, commit_result()} | {:error, term()}
  def commit(snapshot, cache, entry_groups, server \\ __MODULE__) do
    GenServer.call(server, {:commit, snapshot, cache, entry_groups}, :infinity)
  end

  @doc false
  @spec primary_keys(any()) :: [any()]
  def primary_keys(nil), do: [:not_found]
  def primary_keys([]), do: [:not_found]
  def primary_keys(values) when is_list(values), do: Enum.flat_map(values, &primary_keys/1)
  def primary_keys({:ok, value}), do: primary_keys(value)
  def primary_keys({:ok, value, _other}), do: primary_keys(value)
  def primary_keys(%{id: id}), do: [id]
  def primary_keys(_value), do: [:unknown]

  @impl GenServer
  def init(opts) do
    active_table = Keyword.get(opts, :active_table, @active_table)
    :ets.new(active_table, [:named_table, :protected, :set, read_concurrency: true])

    always_ready? = Keyword.get(opts, :ready?, false)
    cache_buster = Process.whereis(CacheBuster)

    state = %{
      active_table: active_table,
      always_ready?: always_ready?,
      ready?: always_ready? or is_pid(cache_buster),
      ready_monitor: monitor_process(cache_buster),
      sessions: %{},
      waiters: %{}
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:begin_snapshot, context, owner}, _from, %{ready?: true} = state) do
    {snapshot, state} = open_session(state, context, owner)
    {:reply, {:ok, snapshot}, state}
  end

  def handle_call({:begin_snapshot, context, owner}, from, state) do
    monitor = Process.monitor(owner)
    waiter = %{context: context, from: from, owner: owner}
    {:noreply, put_in(state.waiters[monitor], waiter)}
  end

  def handle_call({:mark_ready, cache_buster}, _from, state) do
    state =
      state
      |> monitor_cache_buster(cache_buster)
      |> Map.put(:ready?, true)
      |> release_waiters()

    {:reply, :ok, state}
  end

  def handle_call({:invalidate, invalidations}, _from, state) do
    invalidations_by_context =
      Enum.reduce(invalidations, %{}, fn {context, primary_key}, acc ->
        Map.update(acc, context, MapSet.new([primary_key]), &MapSet.put(&1, primary_key))
      end)

    sessions =
      Map.new(state.sessions, fn {snapshot, session} ->
        invalidated =
          case {session.invalidated, Map.fetch(invalidations_by_context, session.context)} do
            {:all, _result} -> :all
            {invalidated, {:ok, primary_keys}} -> MapSet.union(invalidated, primary_keys)
            {invalidated, :error} -> invalidated
          end

        {snapshot, %{session | invalidated: invalidated}}
      end)

    {:reply, :ok, %{state | sessions: sessions}}
  end

  def handle_call({:commit, snapshot, cache, entry_groups}, {owner, _tag}, state) do
    case Map.fetch(state.sessions, snapshot) do
      :error ->
        {:reply, {:error, :unknown_snapshot}, state}

      {:ok, %{owner: session_owner}} when session_owner != owner ->
        {:reply, {:error, :not_owner}, state}

      {:ok, session} ->
        {reply, state} = commit_and_close(state, snapshot, session, cache, entry_groups)
        {:reply, reply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{ready_monitor: monitor} = state) do
    sessions =
      Map.new(state.sessions, fn {snapshot, session} ->
        {snapshot, %{session | invalidated: :all}}
      end)

    {:noreply, %{state | ready?: state.always_ready?, ready_monitor: nil, sessions: sessions}}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.pop(state.waiters, monitor) do
      {nil, waiters} ->
        state = %{state | waiters: waiters}
        {:noreply, close_session_by_monitor(state, monitor)}

      {_waiter, waiters} ->
        {:noreply, %{state | waiters: waiters}}
    end
  end

  defp commit_and_close(state, snapshot, session, cache, entry_groups) do
    reply =
      with :ok <- validate_cache(session.context, cache),
           {:ok, entries, skipped} <- prepare_entries(entry_groups, session.invalidated),
           {:ok, _written?} <- put_many(cache, entries) do
        {:ok, %{written: length(entries), skipped: skipped}}
      end

    {reply, close_session(state, snapshot, session)}
  end

  defp validate_cache(context, cache) do
    if ContextCache.cache_name(context) == cache, do: :ok, else: {:error, :invalid_cache}
  end

  defp prepare_entries(entry_groups, invalidated) when is_list(entry_groups) do
    entry_groups
    |> Enum.reduce_while({[], 0}, fn
      {primary_key, entries}, {acc, skipped} when is_list(entries) ->
        if invalidated?(invalidated, primary_key) do
          {:cont, {acc, skipped + 1}}
        else
          {:cont, {entries ++ acc, skipped}}
        end

      _invalid_group, _acc ->
        {:halt, {:error, :invalid_entries}}
    end)
    |> case do
      {:error, _reason} = error -> error
      {entries, skipped} -> deduplicate_entries(entries, skipped)
    end
  end

  defp prepare_entries(_entry_groups, _invalidated), do: {:error, :invalid_entries}

  defp invalidated?(:all, _dependency), do: true

  defp invalidated?(invalidated, {:any, primary_keys}) do
    MapSet.member?(invalidated, :all) or
      Enum.any?(primary_keys, &MapSet.member?(invalidated, &1))
  end

  defp invalidated?(invalidated, primary_key) do
    MapSet.member?(invalidated, :all) or MapSet.member?(invalidated, primary_key)
  end

  defp deduplicate_entries(entries, skipped) do
    entries
    |> Enum.reduce_while(%{}, fn
      {key, value}, acc ->
        case Map.fetch(acc, key) do
          :error -> {:cont, Map.put(acc, key, value)}
          {:ok, ^value} -> {:cont, acc}
          {:ok, _other_value} -> {:halt, {:error, :conflicting_entries}}
        end

      _invalid_entry, _acc ->
        {:halt, {:error, :invalid_entries}}
    end)
    |> case do
      {:error, _reason} = error -> error
      entries_by_key -> {:ok, Map.to_list(entries_by_key), skipped}
    end
  end

  defp put_many(_cache, []), do: {:ok, true}

  defp put_many(cache, entries) do
    Cachex.put_many(cache, entries)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp open_session(state, context, owner, monitor \\ nil) do
    snapshot = make_ref()
    monitor = monitor || Process.monitor(owner)

    session = %{
      context: context,
      invalidated: MapSet.new(),
      monitor: monitor,
      owner: owner
    }

    increment_active_context(state.active_table, context)
    {snapshot, put_in(state.sessions[snapshot], session)}
  end

  defp close_session(state, snapshot, session) do
    Process.demonitor(session.monitor, [:flush])
    decrement_active_context(state.active_table, session.context)
    %{state | sessions: Map.delete(state.sessions, snapshot)}
  end

  defp close_session_by_monitor(state, monitor) do
    case Enum.find(state.sessions, fn {_snapshot, session} -> session.monitor == monitor end) do
      nil ->
        state

      {snapshot, session} ->
        decrement_active_context(state.active_table, session.context)
        %{state | sessions: Map.delete(state.sessions, snapshot)}
    end
  end

  defp release_waiters(state) do
    Enum.reduce(state.waiters, %{state | waiters: %{}}, fn {monitor, waiter}, state ->
      if Process.alive?(waiter.owner) do
        {snapshot, state} = open_session(state, waiter.context, waiter.owner, monitor)
        GenServer.reply(waiter.from, {:ok, snapshot})
        state
      else
        Process.demonitor(monitor, [:flush])
        state
      end
    end)
  end

  defp monitor_cache_buster(%{always_ready?: true} = state, _cache_buster), do: state

  defp monitor_cache_buster(state, cache_buster) do
    if state.ready_monitor do
      Process.demonitor(state.ready_monitor, [:flush])
    end

    %{state | ready_monitor: Process.monitor(cache_buster)}
  end

  defp monitor_process(pid) when is_pid(pid), do: Process.monitor(pid)
  defp monitor_process(_pid), do: nil

  defp increment_active_context(table, context) do
    :ets.update_counter(table, context, {2, 1}, {context, 0})
  end

  defp decrement_active_context(table, context) do
    case :ets.update_counter(table, context, {2, -1}) do
      0 -> :ets.delete(table, context)
      _remaining -> :ok
    end
  end

  defp active_context?(invalidations) do
    case :ets.whereis(@active_table) do
      :undefined ->
        false

      _table ->
        Enum.any?(invalidations, fn {context, _primary_key} ->
          :ets.member(@active_table, context)
        end)
    end
  end

  defp normalize_invalidations(values) do
    Enum.flat_map(values, fn
      {context, primary_key} ->
        case normalize_primary_key(primary_key) do
          nil -> []
          primary_key -> [{context, primary_key}]
        end

      _invalid ->
        []
    end)
  end

  defp normalize_primary_key(primary_key)
       when is_integer(primary_key) or is_binary(primary_key) or
              primary_key in [:not_found, :unknown],
       do: primary_key

  # Keyword invalidations are interpreted by cache-specific `bust_by/1`
  # callbacks, so the fence cannot safely reduce them to one primary key.
  defp normalize_primary_key(primary_key) when is_list(primary_key), do: :all

  defp normalize_primary_key(%{id: primary_key}), do: primary_key
  defp normalize_primary_key(_primary_key), do: :all
end
