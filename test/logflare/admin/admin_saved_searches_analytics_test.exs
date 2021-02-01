defmodule Logflare.SavedSearches.AnalyticsTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.Factory
  alias Logflare.Lql.{ChartRule, FilterRule}
  alias Logflare.Users
  alias Logflare.SavedSearches
  alias Logflare.SavedSearches.Analytics

  setup do
    {:ok, u} =
      Users.insert_or_update_user(
        params_for(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
      )

    {:ok, s} = Sources.create_source(params_for(:source, token: Faker.UUID.v4(), rules: []), u)

    {:ok, ss} =
      SavedSearches.insert(
        %{
          lql_rules: [
            %ChartRule{
              path: "timestamp",
              value_type: :datetime,
              period: :minute,
              aggregate: :count
            },
            %FilterRule{path: "timestamp", modifiers: %{}, operator: :>=, value: ~D[2020-01-01]}
          ],
          querystring: "t:>=2020-01-01"
        },
        s
      )

    {:ok, ss2} =
      SavedSearches.insert(
        %{
          lql_rules: [
            %ChartRule{
              path: "timestamp",
              value_type: :datetime,
              period: :minute,
              aggregate: :count
            },
            %FilterRule{path: "m.user_id", modifiers: %{}, operator: :<, value: 100},
            %FilterRule{path: "m.node_id", modifiers: %{}, operator: :=, value: "11111-11111"}
          ],
          querystring: "m.user_id:<100 m.node_id:11111-11111"
        },
        s
      )

    {:ok, ss3} =
      SavedSearches.insert(
        %{
          lql_rules: [
            %ChartRule{
              path: "timestamp",
              value_type: :datetime,
              period: :minute,
              aggregate: :count
            },
            %FilterRule{path: "timestamp", modifiers: %{}, operator: :>=, value: ~D[2020-01-02]},
            %FilterRule{
              path: "m.user.login_count",
              modifiers: %{},
              operator: :range_operator,
              value: [1, 10]
            }
          ],
          querystring: "m.user.login_count:1..10 timestamp:>=2020-01-02"
        },
        s
      )

    {:ok, _ss4} =
      SavedSearches.insert(
        %{
          lql_rules: [
            %ChartRule{
              path: "timestamp",
              value_type: :datetime,
              period: :minute,
              aggregate: :count
            },
            %FilterRule{path: "m.context.module", modifiers: %{}, operator: :"~", value: "User"},
            %FilterRule{path: "m.context.line_number", modifiers: %{}, operator: :=, value: 16}
          ],
          querystring: "m.context.module:~User m.context.line_number:16"
        },
        s
      )

    for i <- 1..20 do
      SavedSearches.inc(ss3.id,
        tailing?: false,
        timestamp: %{~U[2020-01-01T00:00:00Z] | day: i, hour: i}
      )

      SavedSearches.inc(ss3.id,
        tailing?: true,
        timestamp: %{~U[2020-01-01T00:00:00Z] | day: i, hour: i}
      )
    end

    for i <- 1..30 do
      SavedSearches.inc(ss3.id, tailing?: false, timestamp: %{~U[2020-01-01T00:00:00Z] | day: i})
      SavedSearches.inc(ss3.id, tailing?: true, timestamp: %{~U[2020-01-01T00:00:00Z] | day: i})
    end

    for i <- 1..10 do
      SavedSearches.inc(ss2.id, tailing?: false, timestamp: %{~U[2020-01-01T00:00:00Z] | day: i})
      SavedSearches.inc(ss2.id, tailing?: true, timestamp: %{~U[2020-01-01T00:00:00Z] | day: i})
    end

    for i <- 1..10 do
      SavedSearches.inc(ss.id, tailing?: false, timestamp: %{~U[2020-01-01T01:00:00Z] | second: i})
    end

    SavedSearches.inc(ss.id, tailing?: false, timestamp: ~U[2020-01-01T01:00:00Z])
    SavedSearches.inc(ss.id, tailing?: false, timestamp: ~U[2020-01-01T01:00:00Z])
    SavedSearches.inc(ss.id, tailing?: false, timestamp: ~U[2020-02-01T01:00:00Z])
    SavedSearches.inc(ss.id, tailing?: false, timestamp: ~U[2020-03-01T01:00:00Z])
    SavedSearches.inc(ss.id, tailing?: true, timestamp: ~U[2020-01-01T01:00:00Z])
    SavedSearches.inc(ss.id, tailing?: true, timestamp: ~U[2020-03-01T01:00:00Z])

    {:ok, sources: [s], users: [u], saved_searches: [ss]}
  end

  describe "SavedSearches.Analytics" do
    test "source_timeseries/0", %{sources: [_s | _], users: [_u | _]} do
      assert Analytics.source_timeseries() == [
               %{count: 1, timestamp: ~D[2020-01-01]},
               %{count: 1, timestamp: ~D[2020-01-02]},
               %{count: 1, timestamp: ~D[2020-01-03]},
               %{count: 1, timestamp: ~D[2020-01-04]},
               %{count: 1, timestamp: ~D[2020-01-05]},
               %{count: 1, timestamp: ~D[2020-01-06]},
               %{count: 1, timestamp: ~D[2020-01-07]},
               %{count: 1, timestamp: ~D[2020-01-08]},
               %{count: 1, timestamp: ~D[2020-01-09]},
               %{count: 1, timestamp: ~D[2020-01-10]},
               %{count: 1, timestamp: ~D[2020-01-11]},
               %{count: 1, timestamp: ~D[2020-01-12]},
               %{count: 1, timestamp: ~D[2020-01-13]},
               %{count: 1, timestamp: ~D[2020-01-14]},
               %{count: 1, timestamp: ~D[2020-01-15]},
               %{count: 1, timestamp: ~D[2020-01-16]},
               %{count: 1, timestamp: ~D[2020-01-17]},
               %{count: 1, timestamp: ~D[2020-01-18]},
               %{count: 1, timestamp: ~D[2020-01-19]},
               %{count: 1, timestamp: ~D[2020-01-20]},
               %{count: 1, timestamp: ~D[2020-01-21]},
               %{count: 1, timestamp: ~D[2020-01-22]},
               %{count: 1, timestamp: ~D[2020-01-23]},
               %{count: 1, timestamp: ~D[2020-01-24]},
               %{count: 1, timestamp: ~D[2020-01-25]},
               %{count: 1, timestamp: ~D[2020-01-26]},
               %{count: 1, timestamp: ~D[2020-01-27]},
               %{count: 1, timestamp: ~D[2020-01-28]},
               %{count: 1, timestamp: ~D[2020-01-29]},
               %{count: 1, timestamp: ~D[2020-01-30]},
               %{count: 1, timestamp: ~D[2020-02-01]},
               %{count: 1, timestamp: ~D[2020-03-01]}
             ]
    end

    test "top_sources/1", %{sources: [_s | _], users: [_u | _], saved_searches: [ss | _]} do
      SavedSearches.inc(ss.id, tailing?: true)
      SavedSearches.inc(ss.id, tailing?: false)
      SavedSearches.inc(ss.id, tailing?: true)
      SavedSearches.inc(ss.id, tailing?: false)
      SavedSearches.inc(ss.id, tailing?: false)
      SavedSearches.inc(ss.id, tailing?: false)

      data = Analytics.top_sources(:"24h")
      assert [%{id: _, name: _, non_tailing_count: 4, tailing_count: 2}] = data
    end

    test "user_timeseries/0", %{sources: [_s | _], users: [_u | _]} do
      assert Analytics.user_timeseries() == [
               %{count: 1, timestamp: ~D[2020-01-01]},
               %{count: 1, timestamp: ~D[2020-01-02]},
               %{count: 1, timestamp: ~D[2020-01-03]},
               %{count: 1, timestamp: ~D[2020-01-04]},
               %{count: 1, timestamp: ~D[2020-01-05]},
               %{count: 1, timestamp: ~D[2020-01-06]},
               %{count: 1, timestamp: ~D[2020-01-07]},
               %{count: 1, timestamp: ~D[2020-01-08]},
               %{count: 1, timestamp: ~D[2020-01-09]},
               %{count: 1, timestamp: ~D[2020-01-10]},
               %{count: 1, timestamp: ~D[2020-01-11]},
               %{count: 1, timestamp: ~D[2020-01-12]},
               %{count: 1, timestamp: ~D[2020-01-13]},
               %{count: 1, timestamp: ~D[2020-01-14]},
               %{count: 1, timestamp: ~D[2020-01-15]},
               %{count: 1, timestamp: ~D[2020-01-16]},
               %{count: 1, timestamp: ~D[2020-01-17]},
               %{count: 1, timestamp: ~D[2020-01-18]},
               %{count: 1, timestamp: ~D[2020-01-19]},
               %{count: 1, timestamp: ~D[2020-01-20]},
               %{count: 1, timestamp: ~D[2020-01-21]},
               %{count: 1, timestamp: ~D[2020-01-22]},
               %{count: 1, timestamp: ~D[2020-01-23]},
               %{count: 1, timestamp: ~D[2020-01-24]},
               %{count: 1, timestamp: ~D[2020-01-25]},
               %{count: 1, timestamp: ~D[2020-01-26]},
               %{count: 1, timestamp: ~D[2020-01-27]},
               %{count: 1, timestamp: ~D[2020-01-28]},
               %{count: 1, timestamp: ~D[2020-01-29]},
               %{count: 1, timestamp: ~D[2020-01-30]},
               %{count: 1, timestamp: ~D[2020-02-01]},
               %{count: 1, timestamp: ~D[2020-03-01]}
             ]
    end

    test "search_timeseries/0", %{sources: [_s | _], users: [_u | _]} do
      assert Analytics.search_timeseries() == [
               %{non_tailing_count: 15, tailing_count: 4, timestamp: ~D[2020-01-01]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-02]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-03]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-04]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-05]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-06]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-07]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-08]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-09]},
               %{non_tailing_count: 3, tailing_count: 3, timestamp: ~D[2020-01-10]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-11]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-12]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-13]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-14]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-15]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-16]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-17]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-18]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-19]},
               %{non_tailing_count: 2, tailing_count: 2, timestamp: ~D[2020-01-20]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-21]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-22]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-23]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-24]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-25]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-26]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-27]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-28]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-29]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-01-30]},
               %{non_tailing_count: 1, tailing_count: 0, timestamp: ~D[2020-02-01]},
               %{non_tailing_count: 1, tailing_count: 1, timestamp: ~D[2020-03-01]}
             ]
    end

    test "saved_searches/0", %{sources: [_s | _], users: [_u | _]} do
      data = Analytics.saved_searches()
      assert data == [%{count: 4, saved_by_user: false}]
    end

    test "top_field_paths/1", %{sources: [_s | _], users: [_u | _]} do
      data = Analytics.top_field_paths(:lql_filters)

      assert data == [
               %{count: 2, path: "timestamp"},
               %{count: 1, path: "m.user_id"},
               %{count: 1, path: "m.context.line_number"},
               %{count: 1, path: "m.user.login_count"},
               %{count: 1, path: "m.node_id"},
               %{count: 1, path: "m.context.module"}
             ]

      data = Analytics.top_field_paths(:lql_charts)
      assert data == [%{count: 4, path: "timestamp"}]
    end

    test "operators/0", %{sources: [_s | _], users: [_u | _]} do
      data = Analytics.operators()

      assert Enum.sort(data) ==
               Enum.sort([
                 %{operator: "=", searches_with_operator_share: 50.0},
                 %{operator: ">=", searches_with_operator_share: 50.0},
                 %{operator: "<", searches_with_operator_share: 25.0},
                 %{operator: "~", searches_with_operator_share: 25.0},
                 %{operator: "range_operator", searches_with_operator_share: 25.0}
               ])
    end
  end
end
