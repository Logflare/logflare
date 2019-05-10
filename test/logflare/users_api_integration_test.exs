defmodule Logflare.Users.APIIntegrationTest do
  @moduledoc false
  import Logflare.Users.API
  alias Logflare.User
  alias Logflare.TableRateCounter, as: TRC
  import Logflare.DummyFactory

  use ExUnit.Case

  setup_all do
    source_id = Faker.UUID.v4()
    source = insert(:source, token: source_id)
    source_id_atom = source.token |> String.to_atom()
    TRC.setup_ets_table(source_id_atom)
    trc = TRC.new(source_id_atom)
    # simulates 100 passed seconds with 10 event/s
    trc_state =
      Enum.reduce(1..100, trc, fn n, acc ->
        TRC.update_state(acc, 10 * n)
      end)

    {:ok, source_id: source_id_atom, source: source, trc: trc_state}
  end

  describe "API context" do
    test "action_allowed?/1 returns true for api_quota of 11 and average rate of 10", %{
      trc: trc,
      source_id: source_id,
      source: source
    } do
      user = insert(:user, api_quota: 11, sources: [source])

      action = %{
        type: {:api_call, :logs_post},
        user: user,
        source_id: source_id
      }

      TRC.update_ets_table(trc)
      assert action_allowed?(action) == :ok
    end

    test "action_allowed?/1 returns true for api_quota of 9 and average rate of 10", %{
      trc: trc,
      source_id: source_id,
      source: source
    } do
      user = insert(:user, api_quota: 9, sources: [source])

      action = %{
        type: {:api_call, :logs_post},
        user: user,
        source_id: source_id
      }

      TRC.update_ets_table(trc)
      TRC.get_avg_rate(source_id)
      assert action_allowed?(action) == {:error, "User rate is over the API quota"}
    end
  end
end
