defmodule Logflare.FetchQueries.FetchQueryWorkerTest do
  use Logflare.DataCase, async: false

  import Logflare.Factory

  alias Logflare.FetchQueries.FetchQueryWorker

  setup do
    {:ok, user: insert(:user)}
  end

  describe "perform/1" do
    test "handles missing fetch query gracefully", %{user: user} do
      job = %Oban.Job{args: %{"fetch_query_id" => 999_999}}

      assert :ok = FetchQueryWorker.perform(job)
    end

    test "logs error when execution fails", %{user: user} do
      backend = insert(:backend, type: :postgres, user: user)
      source = insert(:source, user: user)

      fetch_query = insert(:fetch_query, user: user, backend: backend, source: source)

      job = %Oban.Job{args: %{"fetch_query_id" => fetch_query.id}}

      result = FetchQueryWorker.perform(job)

      # Should error due to unsupported backend
      assert match?({:error, _}, result)
    end
  end
end
