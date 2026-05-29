defmodule LogflareWeb.Plugs.SetTraceUserTest do
  use LogflareWeb.ConnCase, async: false

  require OpenTelemetry.Tracer
  require Record

  alias LogflareWeb.Plugs.SetTraceUser

  @span_fields Record.extract(:span, from: "deps/opentelemetry/include/otel_span.hrl")
  Record.defrecordp(:span, @span_fields)

  setup do
    :ok = :otel_simple_processor.set_exporter(:otel_exporter_pid, self())
    on_exit(fn -> :otel_simple_processor.set_exporter(:none) end)
    :ok
  end

  describe "init/1" do
    test "returns opts unchanged" do
      assert SetTraceUser.init([]) == []
    end
  end

  describe "call/2" do
    test "adds user.id to the current span when a user is assigned", %{conn: conn} do
      user = insert(:user)

      OpenTelemetry.Tracer.with_span "test" do
        conn
        |> assign(:user, user)
        |> SetTraceUser.call([])
      end

      assert_receive {:span, span_record}
      attributes = span_attributes(span_record)
      assert attributes[:"user.id"] == user.id
      refute Map.has_key?(attributes, :"team_user.id")
    end

    test "adds team_user.id when a team user is assigned", %{conn: conn} do
      user = insert(:user)
      team = insert(:team, user: user)
      team_user = insert(:team_user, team: team)

      OpenTelemetry.Tracer.with_span "test" do
        conn
        |> assign(:user, user)
        |> assign(:team_user, team_user)
        |> SetTraceUser.call([])
      end

      assert_receive {:span, span_record}
      attributes = span_attributes(span_record)
      assert attributes[:"user.id"] == user.id
      assert attributes[:"team_user.id"] == team_user.id
    end

    test "passes conn through unchanged when no user is assigned", %{conn: conn} do
      assert SetTraceUser.call(conn, []) == conn
    end

    test "passes conn through unchanged when user assign is nil", %{conn: conn} do
      conn = assign(conn, :user, nil)
      assert SetTraceUser.call(conn, []) == conn
    end
  end

  defp span_attributes(span_record) do
    span_record
    |> span(:attributes)
    |> elem(4)
  end
end
