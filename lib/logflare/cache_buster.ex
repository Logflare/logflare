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
    Cainophile.Adapters.Postgres.subscribe(Logflare.PgPublisher, self())
    {:ok, state}
  end

  def handle_info(%Transaction{changes: changes}, state) do
    for %{relation: {schema, table}} = record <- changes,
        :ok =
          :telemetry.execute([:logflare, :context_cache, :handle_record], %{count: 1}, %{
            schema: schema,
            table: table
          }),
        record = handle_record(record),
        record != :noop do
      :telemetry.execute(
        [:logflare, :context_cache, :busted],
        %{count: 1},
        %{
          schema: schema,
          table: table
        }
      )

      record
    end
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
