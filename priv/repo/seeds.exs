# Script for populating the database. You can run it as:
#
#     mix run priv/repo/seeds.exs
#
# Inside the script, you can read and write to any of your
# repositories directly:
#
#     Logflare.Repo.insert!(%Logflare.SomeSchema{})
#
# We recommend using the bang functions (`insert!`, `update!`
# and so on) as they will fail if something goes wrong.
plans = [
  %{
    name: "Free",
    period: "month",
    price: 0,
    limit_sources: 100,
    limit_rate_limit: 10,
    limit_alert_freq: 14_400_000,
    limit_source_rate_limit: 5,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 259_200_000,
    type: "standard"
  },
  %{
    name: "Hobby",
    period: "month",
    price: 500,
    limit_sources: 100,
    limit_rate_limit: 250,
    limit_alert_freq: 3_600_000,
    limit_source_rate_limit: 50,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 604_800_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Hobby",
    period: "year",
    price: 5000,
    limit_sources: 100,
    limit_rate_limit: 250,
    limit_alert_freq: 3_600_000,
    limit_source_rate_limit: 50,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 604_800_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Pro",
    period: "month",
    price: 800,
    limit_sources: 100,
    limit_rate_limit: 100_000,
    limit_alert_freq: 900_000,
    limit_source_rate_limit: 50000,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 2_592_000_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Pro",
    period: "year",
    price: 8000,
    limit_sources: 100,
    limit_rate_limit: 100_000,
    limit_alert_freq: 900_000,
    limit_source_rate_limit: 50000,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 2_592_000_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Business",
    period: "month",
    price: 1200,
    limit_sources: 100,
    limit_rate_limit: 1000,
    limit_alert_freq: 60000,
    limit_source_rate_limit: 50,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Business",
    period: "year",
    price: 12000,
    limit_sources: 100,
    limit_rate_limit: 1000,
    limit_alert_freq: 60000,
    limit_source_rate_limit: 50,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Enterprise",
    period: "year",
    price: 20000,
    limit_sources: 100,
    limit_rate_limit: 5000,
    limit_alert_freq: 1000,
    limit_source_rate_limit: 100,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Enterprise",
    period: "month",
    price: 2000,
    limit_sources: 100,
    limit_rate_limit: 5000,
    limit_alert_freq: 1000,
    limit_source_rate_limit: 100,
    limit_saved_search_limit: 1,
    limit_team_users_limit: 2,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Lifetime",
    period: "life",
    price: 50000,
    limit_sources: 8,
    limit_rate_limit: 250,
    limit_alert_freq: 60000,
    limit_source_rate_limit: 25,
    limit_saved_search_limit: 10,
    limit_team_users_limit: 9,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "standard"
  },
  %{
    name: "Enterprise Metered BYOB",
    period: "month",
    price: 10000,
    limit_sources: 100,
    limit_rate_limit: 1000,
    limit_alert_freq: 60000,
    limit_source_rate_limit: 1000,
    limit_saved_search_limit: 10,
    limit_team_users_limit: 10,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "metered"
  },
  %{
    name: "Enterprise Metered",
    period: "month",
    price: 10000,
    limit_sources: 100,
    limit_rate_limit: 1000,
    limit_alert_freq: 60000,
    limit_source_rate_limit: 1000,
    limit_saved_search_limit: 10,
    limit_team_users_limit: 10,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "metered"
  },
  %{
    name: "Metered",
    period: "month",
    price: 1500,
    limit_sources: 100,
    limit_rate_limit: 1000,
    limit_alert_freq: 60000,
    limit_source_rate_limit: 1000,
    limit_saved_search_limit: 10,
    limit_team_users_limit: 10,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "metered"
  },
  %{
    name: "Metered BYOB",
    period: "month",
    price: 1000,
    limit_sources: 100,
    limit_rate_limit: 1000,
    limit_alert_freq: 60000,
    limit_source_rate_limit: 1000,
    limit_saved_search_limit: 10,
    limit_team_users_limit: 10,
    limit_source_fields_limit: 500,
    limit_source_ttl: 5_184_000_000,
    limit_key_values: 10_000_000,
    type: "metered"
  }
]

Enum.each(plans, fn plan ->
  Logflare.Repo.insert!(Logflare.Billing.Plan.changeset(%Logflare.Billing.Plan{}, plan))
end)
