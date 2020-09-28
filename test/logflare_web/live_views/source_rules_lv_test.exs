defmodule LogflareWeb.Source.RulesLqlTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  @endpoint LogflareWeb.Endpoint
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Lql.FilterRule
  alias Logflare.Repo
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Rule
  alias Logflare.Users
  import Logflare.Factory
  alias Logflare.Plans
  alias Logflare.Plans.Plan
  use Mimic
  

  setup_all do
    Sources.Counters.start_link()
    :ok
  end

  describe "LQL rules" do
    setup :set_mimic_global

    setup do
      stub(Plans, :get_plan_by_user, fn _ -> %Plan{limit_source_fields_limit: 500} end)
      user = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
      user = Users.get(user.id)

      source = params_for(:source)
      {:ok, source} = Sources.create_source(source, user)
      Sources.Cache.put_bq_schema(source.token, SchemaBuilder.initial_table_schema())

      {:ok, sink} =
        :source
        |> params_for(name: "Sink Source 1")
        |> Sources.create_source(user)

      rls = %RLS{source_id: source.token, source: source}

      {:ok, _pid} = RLS.start_link(rls)

      Process.sleep(100)
      %{sources: [source, sink], user: [user]}
    end

    test "mount with source owner user", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u.id})
        |> assign(:user, u)
        |> get("/sources/#{s.id}/rules")

      assert {:ok, view, html} = live(conn)
      assert html =~ ~S|id="lql-rules-container"|

      assert render_submit(view, :fsubmit, %{
               "rule" => %{"lql_string" => "123errorstring", "sink" => sink.token}
             }) =~ "Sink Source 1"

      html =
        render_submit(view, :fsubmit, %{
          "rule" => %{"lql_string" => "123infostring", "sink" => sink.token}
        })

      assert html =~ "123infostring"
      assert html =~ "123errorstring"
    end

    test "mount with admin owner", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      rule =
        insert(:rule,
          sink: sink.token,
          source_id: s.id,
          lql_filters: [
            %FilterRule{operator: "~", path: "message", modifiers: %{}, value: "info"},
            lql_string: "message:info"
          ]
        )

      user = insert(:user, email: "example@example.org", admin: true)
      user = Users.get(user.id)

      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u.id})
        |> assign(:user, user)
        |> get("/sources/#{s.id}/rules")

      assert {:ok, view, html} = live(conn)
      assert html =~ ~S|id="lql-rules-container"|

      assert render_submit(view, :fsubmit, %{
               "rule" => %{"lql_string" => "123errorstring", "sink" => sink.token}
             }) =~ "Sink Source 1"

      html =
        render_submit(view, :fsubmit, %{
          "rule" => %{"lql_string" => "123infostring", "sink" => sink.token}
        })

      assert html =~ "123infostring"
      assert html =~ "123errorstring"
    end

    test "add and delete rules", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      {:ok, view, _html} =
        conn
        |> assign(:user, u)
        |> Plug.Test.init_test_session(%{user_id: u.id})
        |> get("/sources/#{s.id}/rules")
        |> live()

      assert render_submit(view, :fsubmit, %{
               "rule" => %{"lql_string" => "123errorstring", "sink" => sink.token}
             }) =~ "Sink Source 1"

      html =
        render_submit(view, :fsubmit, %{
          "rule" => %{"lql_string" => "123infostring", "sink" => sink.token}
        })

      assert html =~ "123infostring"
      assert html =~ "123errorstring"

      source = Sources.get_by_and_preload(token: s.token)
      assert length(source.rules) == 2

      rule_id =
        source.rules
        |> Enum.find(&(&1.lql_string == "123infostring"))
        |> Map.get(:id)
        |> to_string()

      html = render_click(view, "delete_rule", %{"rule_id" => rule_id})

      refute html =~ rule_id
      source = Sources.get_by_and_preload(token: s.token)
      assert length(source.rules) == 1
    end

    test "mount with non-existing source", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u.id})
        |> assign(:user, u)
        |> get("/sources/#{s.id + 1000}/rules")

      assert html_response(conn, 404) =~ "404"
    end
  end

  describe "Rule regex to LQL upgrade" do
    setup do
      user = Users.get_by(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))

      source = params_for(:source)

      {:ok, source} = Sources.create_source(source, user)

      {:ok, sink} =
        :source
        |> params_for(name: "Sink Source 1")
        |> Sources.create_source(user)

      Repo.insert(%Rule{
        regex: "100",
        regex_struct: Regex.compile!("100"),
        source_id: source.id,
        sink: sink.token
      })

      {:ok, sink2} =
        :source
        |> params_for(name: "Sink Source 2")
        |> Sources.create_source(user)

      Repo.insert(%Rule{
        regex: ~S"\d\d",
        regex_struct: Regex.compile!(~S"\d\d"),
        source_id: source.id,
        sink: sink2.token
      })

      {:ok, sink3} =
        :source
        |> params_for(name: "Sink Source 3")
        |> Sources.create_source(user)

      Repo.insert(%Rule{
        regex: ~S"\w+",
        regex_struct: Regex.compile!(~S"\w+"),
        source_id: source.id,
        sink: sink3.token
      })

      %{sources: [source, sink, sink2, sink3], user: [user]}
    end

    test "is successfull", %{sources: [source | _], user: [user], conn: conn} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: user.id})
        |> assign(:user, user)
        |> get("/sources/#{source.id}/rules")

      assert {:ok, view, html} = live(conn)

      assert html =~ ~S|id="lql-rules-container"|

      assert html =~ "#has_regex_rules"

      html = render_click(view, "upgrade_rules", %{})

      assert [
               %Logflare.Rule{
                 lql_filters: [
                   %Logflare.Lql.FilterRule{
                     modifiers: %{quoted_string: true},
                     operator: :string_contains,
                     path: "event_message",
                     value: "100",
                     shorthand: nil,
                     values: nil
                   }
                 ],
                 lql_string: ~S|"100"|,
                 regex: "100",
                 regex_struct: ~r/100/
               },
               %Logflare.Rule{
                 lql_filters: [
                   %Logflare.Lql.FilterRule{
                     modifiers: %{},
                     operator: :string_contains,
                     path: "event_message",
                     value: ~S"\d\d"
                   }
                 ],
                 lql_string: ~S|"\d\d"|,
                 regex: ~S"\d\d",
                 regex_struct: ~r/\d\d/
               },
               %Logflare.Rule{
                 lql_filters: [
                   %Logflare.Lql.FilterRule{
                     modifiers: %{},
                     operator: :string_contains,
                     path: "event_message",
                     value: ~S"\w+"
                   }
                 ],
                 lql_string: ~S|"\w+"|,
                 regex: ~S"\w+",
                 regex_struct: ~r/\w+/
               }
             ] = Sources.get_by_and_preload(id: source.id).rules

      refute html =~ "#has_regex_rules"
    end
  end
end
