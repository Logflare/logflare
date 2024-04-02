defmodule Logflare.CacheBuster do
  @moduledoc """
    Monitors our Postgres replication log and busts the cache accordingly.
  """

  use GenServer

  require Logger

  alias Logflare.ContextCache
  alias Cainophile.Changes.{NewRecord, UpdatedRecord, DeletedRecord, Transaction}

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    Logger.put_process_level(self(), :error)

    Cainophile.Adapters.Postgres.subscribe(Logflare.PgPublisher, self())
    {:ok, state}
  end

  @doc """
  Sets the Logger level for this process. It's started with level :error.

  To debug wal records set process to level :info and each transaction will be logged.
  """

  @spec set_log_level(Logger.levels()) :: :ok
  def set_log_level(level) when is_atom(level) do
    GenServer.call(__MODULE__, {:put_level, level})
  end

  def handle_call({:put_level, level}, _from, state) do
    :ok = Logger.put_process_level(self(), level)

    {:reply, :ok, state}
  end

  def handle_info(%Transaction{changes: []}, state), do: {:noreply, state}

  def handle_info(%Transaction{changes: changes} = transaction, state) do
    Logger.info("WAL record received: #{inspect(transaction)}")

    for record <- changes,
        record = handle_record(record),
        record != :noop do
      record
    end
    |> tap(fn
      [] ->
        nil

      records ->
        :telemetry.execute([:logflare, :cache_buster, :to_bust], %{count: length(records)})
    end)
    |> ContextCache.bust_keys()

    {:noreply, state}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "sources"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Sources, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Users, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Billing, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "plans"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Billing, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.SourceSchemas, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "backends"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Backends, String.to_integer(id)}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Logflare.Billing, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Logflare.SourceSchemas, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "sources"},
         record: %{"id" => _id, "user_id" => user_id}
       })
       when is_binary(user_id) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Logflare.Sources, :not_found}
    # {Logflare.Users, String.to_integer(user_id)}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "rules"},
         record: %{"id" => _id, "source_id" => source_id}
       })
       when is_binary(source_id) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Logflare.Sources, String.to_integer(source_id)}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "users"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Logflare.Users, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "backends"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Logflare.Backends, :not_found}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "billing_accounts"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Billing, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "sources"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Sources, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "source_schemas"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.SourceSchemas, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "users"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Users, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "backends"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Logflare.Backends, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "rules"},
         old_record: %{"id" => _id, "source_id" => source_id}
       })
       when is_binary(source_id) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {Logflare.Sources, String.to_integer(source_id)}
  end

  defp handle_record(_record) do
    :noop
  end
end
