defmodule Logflare.Commons do
  defmacro __using__(_ctx) do
    quote do
      alias Logflare.Repo
      alias Logflare.BqRepo
      alias Logflare.MemoryRepo
      alias Logflare.RepoWithCache

      alias Logflare.Source
      alias Logflare.Sources

      alias Logflare.User
      alias Logflare.Users
      alias Logflare.Users.UserPreferences

      alias Logflare.Teams
      alias Logflare.Teams.Team
      alias Logflare.TeamUsers
      alias Logflare.TeamUsers.TeamUser

      alias Logflare.BillingAccounts
      alias Logflare.BillingAccounts.BillingAccount

      alias Logflare.BillingCounts.BillingCount
      alias Logflare.Plans
      alias Logflare.Plans.Plan

      alias Logflare.Rule

      alias Logflare.Lql

      alias Logflare.SavedSearches
      alias Logflare.SavedSearch

      alias Logflare.Sources.SourceSchema
      alias Logflare.Sources.SourceSchema

      alias Logflare.Logs
      alias Logflare.Logs.LogEvents.SearchResult
      alias Logflare.Logs.LogEvents
      alias Logflare.LogEvent
      alias Logflare.LogEvent, as: LE

      alias Logflare.SystemMetrics

      alias Logflare.EctoQueryBQ

      alias Logflare.DateTimeUtils

      alias Logflare.Source.RecentLogsServer, as: RLS

      alias Logflare.PubSubRates

      alias Logflare.AccountEmail
      alias Logflare.Mailer

      alias Logflare.JSON

      alias Logflare.Tracker

      alias Logflare.EctoSchemaReflection
      alias Logflare.EctoChangesetExtras
    end
  end
end

defmodule LogflareWeb.Commons do
  defmacro __using__(_ctx) do
    alias LogflareWeb.SharedView
  end
end
