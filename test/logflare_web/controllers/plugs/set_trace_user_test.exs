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

  describe "call/2" do
    test "adds user id and email to the current span when a user is assigned", %{conn: conn} do
      user = insert(:user)

      OpenTelemetry.Tracer.with_span "test" do
        conn
        |> assign(:user, user)
        |> SetTraceUser.call([])
      end

      assert_receive {:span, span_record}
      attributes = span_attributes(span_record)
      assert attributes[:user_id] == user.id
      assert attributes[:user_email] == user.email
      refute Map.has_key?(attributes, :team_user_id)
    end

    test "adds team user id and email when a team user is assigned", %{conn: conn} do
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
      assert attributes[:user_id] == user.id
      assert attributes[:user_email] == user.email
      assert attributes[:team_user_id] == team_user.id
      assert attributes[:team_user_email] == team_user.email
    end

    test "passes conn through unchanged when no user is assigned", %{conn: conn} do
      assert SetTraceUser.call(conn, []) == conn

      conn_with_nil_user = assign(conn, :user, nil)
      assert SetTraceUser.call(conn_with_nil_user, []) == conn_with_nil_user
    end
  end

  defp span_attributes(span_record) do
    span_record
    |> span(:attributes)
    |> elem(4)
  end
end
