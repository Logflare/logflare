defmodule LogflareWeb.MonacoEditorComponentTest do
  use LogflareWeb.ConnCase, async: true

  alias LogflareWeb.MonacoEditorComponent

  import Phoenix.Component, only: [to_form: 1]

  describe "rendering the live component" do
    setup do
      form = %{"query" => ""} |> to_form()
      [assigns: %{form: form}]
    end

    test "renders the live component", %{assigns: assigns} do
      html =
        render_component(MonacoEditorComponent, %{
          id: "test-editor",
          field: assigns.form[:query],
          endpoints: [],
          sources: [],
          alerts: []
        })

      assert html =~ ~s(phx-hook="CodeEditorHook")
    end

    test "renders with completions set", %{assigns: assigns} do
      html =
        render_component(MonacoEditorComponent, %{
          id: "test-editor",
          field: assigns.form[:query],
          endpoints: [%{id: "endpoint1", name: "Endpoint 1"}],
          sources: [%{id: "source1", name: "Source 1"}],
          alerts: []
        })

      assert html =~ ~s(<script phx-update="ignore" id="query_completions">)
      assert html =~ ~s(const completions = ["Source 1","Endpoint 1"])
    end

    test "renders with parser error message set", %{assigns: assigns} do
      html =
        render_component(MonacoEditorComponent, %{
          id: "test-editor",
          field: assigns.form[:query],
          endpoints: [],
          sources: [],
          alerts: [],
          parse_error_message: "Invalid SQL syntax"
        })

      assert html =~ ~s(<div class="alert alert-warning)
      assert html =~ "Invalid SQL syntax"
    end

    test "renders field value when set" do
      form = %{"query" => "SELECT * FROM MyApp.Logs"} |> to_form()

      html =
        render_component(MonacoEditorComponent, %{
          id: "test-editor",
          field: form[:query],
          endpoints: [],
          sources: [],
          alerts: []
        })

      assert html =~ "SELECT * FROM MyApp.Logs"
    end
  end

  describe "parsing the query" do
    test "parse_query/3 returns :ok for valid query" do
      query = "SELECT * FROM `MyApp.Logs` WHERE timestamp > '2023-01-01'"
      endpoints = []
      alerts = []

      assert MonacoEditorComponent.parse_query(query, endpoints, alerts) == :ok
    end

    test "parse_query/3 returns {:error, message} for invalid query" do
      query = "INVALID SQL QUERY"
      endpoints = []
      alerts = []

      assert {:error, message} = MonacoEditorComponent.parse_query(query, endpoints, alerts)
      assert is_binary(message)
    end

    test "parse_query/3 ignores empty query" do
      query = ""
      endpoints = []
      alerts = []

      assert :ok = MonacoEditorComponent.parse_query(query, endpoints, alerts)
    end

    test "parse_query/3 calls on_query_change callback", %{conn: conn} do
      defmodule EditorLive do
        use Phoenix.LiveView

        def mount(_params, %{"parent" => parent}, socket) do
          {:ok, assign(socket, parent: parent, form: %{"query" => "select 1"} |> to_form())}
        end

        def render(assigns) do
          ~H"""
          <.live_component
            module={LogflareWeb.MonacoEditorComponent}
            id="test-editor"
            field={@form[:query]}
            endpoints={[]}
            sources={[]}
            alerts={[]}
            on_query_change={fn query -> send(@parent, {:query_changed, query}) end}
          />
          """
        end
      end

      parent = self()
      {:ok, view, _html} = live_isolated(conn, EditorLive, session: %{"parent" => parent})

      view
      |> with_target("#test-editor")
      |> render_change("parse-query", %{"value" => "select 2"})

      assert_received {:query_changed, "select 2"}
    end
  end
end
