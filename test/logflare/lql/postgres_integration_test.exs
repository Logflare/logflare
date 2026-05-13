defmodule Logflare.Lql.PostgresIntegrationTest do
  use Logflare.DataCase

  import Ecto.Query

  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Lql
  alias Logflare.Lql.Rules.SelectRule

  @pg_table "log_events"

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    {:ok, source: source, user: user}
  end

  describe "select rules with aliases" do
    test "includes 'as' in SQL" do
      query = from(l in @pg_table, select: %{})
      select_rule = %SelectRule{path: "event_message", alias: "msg"}

      result = Lql.apply_select_rules(query, [select_rule], dialect: :postgres)

      {:ok, {sql, _params}} = PostgresAdaptor.ecto_to_sql(result, [])
      assert sql |> String.downcase() =~ ~s|as "msg"|
    end

    test "nested field includes 'as' in SQL" do
      query = from(l in @pg_table, select: %{})
      select_rule = %SelectRule{path: "metadata.user.id", alias: "user_id"}

      result = Lql.apply_select_rules(query, [select_rule], dialect: :postgres)

      {:ok, {sql, _params}} = PostgresAdaptor.ecto_to_sql(result, [])
      assert sql |> String.downcase() =~ ~s|as "user_id"|
    end

    test "field without alias does not include 'as' keyword in SQL" do
      query = from(l in @pg_table, select: %{})
      select_rule = %SelectRule{path: "event_message", alias: nil}

      result = Lql.apply_select_rules(query, [select_rule], dialect: :postgres)

      {:ok, {sql, _params}} = PostgresAdaptor.ecto_to_sql(result, [])
      assert sql |> String.downcase() =~ "event_message"
      refute sql |> String.downcase() =~ ~s|as "event_message"|
    end
  end
end
