defmodule Logflare.CacheBuster do
  @moduledoc """
    Monitors our Postgres replication log and busts the cache accordingly.
  """

  use GenServer

  require Logger

  alias Logflare.ContextCache

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    Cainophile.Adapters.Postgres.subscribe(Logflare.PgPublisher, self())
    {:ok, state}
  end

  def handle_info(%Cainophile.Changes.Transaction{changes: changes}, state) do
    for record <- changes do
      handle_record(record)
    end

    {:noreply, state}
  end

  defp handle_record(%{relation: {"public", "sources"}, record: %{"id" => id}})
       when is_binary(id) do
    ContextCache.bust_keys(Logflare.Sources, String.to_integer(id))
  end

  defp handle_record(%{relation: {"public", "users"}, record: %{"id" => id}})
       when is_binary(id) do
    ContextCache.bust_keys(Logflare.Users, String.to_integer(id))
  end

  defp handle_record(%{relation: {"public", "billing_accounts"}, record: %{"id" => id}})
       when is_binary(id) do
    ContextCache.bust_keys(Logflare.BillingAccounts, String.to_integer(id))
  end

  defp handle_record(_record) do
    :noop
  end
end
