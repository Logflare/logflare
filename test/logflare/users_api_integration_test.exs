defmodule Logflare.Users.APIIntegrationTest do
  @moduledoc false
  import Logflare.Users.API
  alias Logflare.User
  alias Logflare.SourceRateCounter, as: SRC
  import Logflare.DummyFactory

  use Logflare.DataCase

  @moduletag integration: true

  setup do
    source_id = Faker.UUID.v4()
    source = insert(:source, token: source_id)
    source_id_atom = source.token
    SRC.setup_ets_table(source_id_atom)
    src = SRC.new(source_id_atom)
    # simulates 100 passed seconds with 10 event/s
    src_state =
      Enum.reduce(1..100, src, fn n, acc ->
        SRC.update_state(acc, 10 * n)
      end)

    {:ok, source: source, src: src_state}
  end

  describe "API context" do
    test "action_allowed?/1 returns true for api_quota of 11 and average rate of 10", %{
      src: src,
      source: source
    } do
      user = insert(:user, api_quota: 11, sources: [source])

      action = %{
        type: {:api_call, :logs_post},
        user: user,
        source: source
      }

      SRC.update_ets_table(src)

      assert verify_api_rates_quotas(action) ==
               {:ok,
                %{
                  message: nil,
                  metrics: %{
                    source: %{limit: 3000, remaining: 2400},
                    user: %{limit: 660, remaining: 60}
                  }
                }}
    end

    test "action_allowed?/1 returns true for api_quota of 9 and average rate of 10", %{
      src: src,
      source_id: source_id,
      source: source
    } do
      user = insert(:user, api_quota: 9, sources: [source])

      action = %{
        type: {:api_call, :logs_post},
        user: user,
        source_id: source_id
      }

      SRC.update_ets_table(src)
      SRC.get_avg_rate(source_id)

      assert verify_api_rates_quotas(action) ==
               {:error,
                %{
                  message:
                    "User rate is over the API quota. Email support@logflare.app to increase your rate limit.",
                  metrics: %{
                    source: %{limit: 3000, remaining: 2400},
                    user: %{limit: 540, remaining: -60}
                  }
                }}
    end
  end
end
