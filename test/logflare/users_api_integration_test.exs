defmodule Logflare.Users.APIIntegrationTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.Users.API
  alias Logflare.Sources.Source.RateCounterServer, as: SRC
  alias Logflare.Sources
  alias Logflare.Billing.Plan

  @moduletag :skip

  setup do
    user = insert(:user)
    source_id = TestUtils.gen_uuid()
    source = insert(:source, token: source_id, user: user)
    source_id_atom = source.token
    # SRC.setup_ets_table(source_id_atom)
    src = SRC.new(source_id_atom)
    # simulates 100 passed seconds with 10 event/s
    src_state =
      Enum.reduce(1..100, src, fn n, acc ->
        SRC.update_state(acc, 10 * n)
      end)

    source = Sources.get(String.to_atom(source_id))
    {:ok, source: source, src: src_state}
  end

  describe "API context" do
    test "action_allowed?/1 returns true for api_quota of 11 and average rate of 10", %{
      src: src,
      source: source
    } do
      user = insert(:user, api_quota: 11, sources: [source])

      plan = %Plan{
        limit_source_rate_limit: 10,
        limit_rate_limit: 11
      }

      action = %{
        type: {:api_call, :logs_post},
        user: user,
        source: source,
        plan: plan
      }

      SRC.update_ets_table(src)

      assert verify_api_rates_quotas(action) ==
               {
                 :error,
                 %{
                   message:
                     "Source rate is over the API quota. Email support@logflare.app to increase your rate limit.",
                   metrics: %{
                     source: %{limit: 3000, remaining: -5_997_000},
                     user: %{limit: 660, remaining: -5_999_340}
                   }
                 }
               }
    end

    test "action_allowed?/1 returns true for api_quota of 9 and average rate of 10", %{
      src: src,
      source: source
    } do
      user = insert(:user, api_quota: 9, sources: [source])

      action = %{
        type: {:api_call, :logs_post},
        user: user,
        source: source
      }

      SRC.update_ets_table(src)
      SRC.get_avg_rate(source.token)

      assert verify_api_rates_quotas(action) ==
               {
                 :error,
                 %{
                   message:
                     "Source rate is over the API quota. Email support@logflare.app to increase your rate limit.",
                   metrics: %{
                     source: %{limit: 3000, remaining: -5_997_000},
                     user: %{limit: 540, remaining: -5_999_460}
                   }
                 }
               }
    end
  end
end
