defmodule LogflareWeb.Source.RulesLqlTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Repo
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Rule
  alias Logflare.Users
  @endpoint LogflareWeb.Endpoint
  import Logflare.Factory
  use Placebo

  describe "LQL rules" do
    setup do
      user = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
      user = Users.get(user.id)

      source = params_for(:source)
      {:ok, source} = Sources.create_source(source, user)

      {:ok, sink} =
        :source
        |> params_for(name: "Sink Source 1")
        |> Sources.create_source(user)

      {:ok, _} = Sources.Counters.start_link()
      {:ok, _pid} = RLS.start_link(%RLS{source_id: source.token})

      Process.sleep(2000)
      %{sources: [source, sink], user: [user]}
    end

    test "mount", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      conn =
        conn
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

    test "add and delete rules", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      {:ok, view, _html} =
        conn
        |> assign(:user, u)
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
      html = render_click(view, "delete_rule", %{"rule_id" => to_string(hd(source.rules).id)})

      refute html =~ "123infostring"
      source = Sources.get_by_and_preload(token: s.token)
      assert length(source.rules) == 1
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

    test "is successfull", %{sources: [source | _], user: [user]} do
      conn =
        conn
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
                     modifiers: [],
                     operator: :"~",
                     path: "event_message",
                     value: ~S"\w+"
                   }
                 ],
                 lql_string: ~S|"\w+"|,
                 regex: ~S"\w+",
                 regex_struct: ~r/\w+/
               },
               %Logflare.Rule{
                 lql_filters: [
                   %Logflare.Lql.FilterRule{
                     modifiers: [],
                     operator: :"~",
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
                     modifiers: [],
                     operator: :"~",
                     path: "event_message",
                     value: "100"
                   }
                 ],
                 lql_string: ~S|"100"|,
                 regex: "100",
                 regex_struct: ~r/100/
               }
             ] = Sources.get_by_and_preload(id: source.id).rules

      refute html =~ "#has_regex_rules"
    end
  end
end
