defmodule Logflare.Users.PreloadDefaultsTest do
  use Logflare.DataCase, async: false

  alias Logflare.Users

  describe "preload_defaults/1 for a list of users" do
    test "does not re-query the users table to hydrate source retention_days" do
      insert(:plan)

      users =
        for _ <- 1..5 do
          user = insert(:user)
          insert_list(3, :source, user: user)
          user
        end

      count = count_queries(["users"], fn -> Users.preload_defaults(users) end)

      assert count == 0
    end

    test "resolves plans in a bounded number of queries regardless of list size" do
      insert(:plan)

      count_billing_queries_for = fn user_count ->
        users = for _ <- 1..user_count, do: insert(:user)

        count_queries(["billing_accounts", "plans", "partner_users"], fn ->
          Users.preload_defaults(users)
        end)
      end

      count_for_one = count_billing_queries_for.(1)
      count_for_twenty = count_billing_queries_for.(20)

      assert count_for_one == count_for_twenty
    end
  end

  defp count_queries(sources, fun) do
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    handler_id = {:preload_defaults_query_count, make_ref()}

    :telemetry.attach(
      handler_id,
      [:logflare, :repo, :query],
      fn _event, _measurements, metadata, _config ->
        if metadata.source in sources do
          Agent.update(counter, &(&1 + 1))
        end
      end,
      nil
    )

    try do
      fun.()
      Agent.get(counter, & &1)
    after
      :telemetry.detach(handler_id)
      Agent.stop(counter)
    end
  end
end
