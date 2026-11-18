defmodule Logflare.IngressGate do
  @moduledoc """
  Stops admission before the final shutdown flush, including requests on existing connections.

  Bandit leases last until its request stop/exception event, after response completion.
  RPC leases cover the server callback; Cowboy handles the remaining wire-level shutdown.
  Write leases cover the ingestion handoff and can outlive the request wait.
  """

  @http_key {__MODULE__, :http_request}
  @events [
    [:bandit, :request, :start],
    [:bandit, :request, :stop],
    [:bandit, :request, :exception]
  ]

  @spec initialize() :: :ok
  def initialize do
    :ets.new(__MODULE__, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.insert(__MODULE__, {:admission, :open})

    case :telemetry.attach_many(__MODULE__, @events, &__MODULE__.handle_event/4, nil) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end

  @spec enter() :: {:ok, reference()} | :closed
  def enter, do: acquire(:request)

  @spec with_write((-> result)) :: result | {:error, :source_unavailable} when result: var
  def with_write(fun) do
    case acquire(:write) do
      {:ok, token} ->
        try do
          fun.()
        after
          leave(token)
        end

      :closed ->
        {:error, :source_unavailable}
    end
  end

  defp acquire(kind) do
    token = make_ref()
    :ets.insert(__MODULE__, {token, kind, self()})

    case state() do
      :open ->
        {:ok, token}

      :quiescing when kind == :write ->
        {:ok, token}

      _ ->
        leave(token)
        :closed
    end
  end

  @spec leave(reference()) :: :ok
  def leave(token) do
    :ets.delete(__MODULE__, token)
    :ok
  end

  @spec open?() :: boolean()
  def open?, do: state() == :open

  @spec quiesce() :: :ok
  def quiesce do
    :ets.select_replace(__MODULE__, [{{:admission, :open}, [], [{{:admission, :quiescing}}]}])
    :ok
  end

  @spec seal() :: :ok
  def seal do
    :ets.insert(__MODULE__, {:admission, :sealed})
    :ok
  end

  defp state, do: :ets.lookup_element(__MODULE__, :admission, 2, :sealed)

  @spec http_admitted?() :: boolean()
  def http_admitted? do
    case Process.get(@http_key) do
      {_span, {:ok, _token}} -> true
      {_span, :closed} -> false
      nil -> open?()
    end
  end

  @spec active_count() :: non_neg_integer()
  def active_count, do: active_count(:request)

  defp active_count(kind) do
    :ets.foldl(
      fn
        {token, ^kind, pid}, count ->
          if Process.alive?(pid) do
            count + 1
          else
            leave(token)
            count
          end

        _entry, count ->
          count
      end,
      0,
      __MODULE__
    )
  end

  @spec await_empty(integer()) :: :drained | :timeout
  def await_empty(deadline), do: await_empty(:request, deadline)

  @spec await_writes(integer()) :: :drained | :timeout
  def await_writes(deadline), do: await_empty(:write, deadline)

  defp await_empty(kind, deadline) do
    cond do
      active_count(kind) == 0 ->
        :drained

      System.monotonic_time(:millisecond) >= deadline ->
        :timeout

      true ->
        Process.sleep(min(10, max(deadline - System.monotonic_time(:millisecond), 0)))
        await_empty(kind, deadline)
    end
  end

  @doc false
  @spec handle_event([atom()], map(), map(), term()) :: :ok
  def handle_event([:bandit, :request, :start], _measurements, metadata, _config) do
    admission = enter()
    Process.put(@http_key, {metadata.telemetry_span_context, admission})
    :ok
  end

  def handle_event(_event, _measurements, metadata, _config) do
    case Process.get(@http_key) do
      {span, admission} when span == metadata.telemetry_span_context ->
        Process.delete(@http_key)

        case admission do
          {:ok, token} -> leave(token)
          :closed -> :ok
        end

      _ ->
        :ok
    end
  end
end
