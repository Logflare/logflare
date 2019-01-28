defmodule LogflareWeb.RuleControllerTest do
  use LogflareWeb.ConnCase

  alias Logflare.Sources

  @create_attrs %{regex: "some regex"}
  @update_attrs %{regex: "some updated regex"}
  @invalid_attrs %{regex: nil}

  def fixture(:rule) do
    {:ok, rule} = Sources.create_rule(@create_attrs)
    rule
  end

  describe "index" do
    test "lists all rules", %{conn: conn} do
      conn = get(conn, Routes.rule_path(conn, :index))
      assert html_response(conn, 200) =~ "Listing Rules"
    end
  end

  describe "new rule" do
    test "renders form", %{conn: conn} do
      conn = get(conn, Routes.rule_path(conn, :new))
      assert html_response(conn, 200) =~ "New Rule"
    end
  end

  describe "create rule" do
    test "redirects to show when data is valid", %{conn: conn} do
      conn = post(conn, Routes.rule_path(conn, :create), rule: @create_attrs)

      assert %{id: id} = redirected_params(conn)
      assert redirected_to(conn) == Routes.rule_path(conn, :show, id)

      conn = get(conn, Routes.rule_path(conn, :show, id))
      assert html_response(conn, 200) =~ "Show Rule"
    end

    test "renders errors when data is invalid", %{conn: conn} do
      conn = post(conn, Routes.rule_path(conn, :create), rule: @invalid_attrs)
      assert html_response(conn, 200) =~ "New Rule"
    end
  end

  describe "edit rule" do
    setup [:create_rule]

    test "renders form for editing chosen rule", %{conn: conn, rule: rule} do
      conn = get(conn, Routes.rule_path(conn, :edit, rule))
      assert html_response(conn, 200) =~ "Edit Rule"
    end
  end

  describe "update rule" do
    setup [:create_rule]

    test "redirects when data is valid", %{conn: conn, rule: rule} do
      conn = put(conn, Routes.rule_path(conn, :update, rule), rule: @update_attrs)
      assert redirected_to(conn) == Routes.rule_path(conn, :show, rule)

      conn = get(conn, Routes.rule_path(conn, :show, rule))
      assert html_response(conn, 200) =~ "some updated regex"
    end

    test "renders errors when data is invalid", %{conn: conn, rule: rule} do
      conn = put(conn, Routes.rule_path(conn, :update, rule), rule: @invalid_attrs)
      assert html_response(conn, 200) =~ "Edit Rule"
    end
  end

  describe "delete rule" do
    setup [:create_rule]

    test "deletes chosen rule", %{conn: conn, rule: rule} do
      conn = delete(conn, Routes.rule_path(conn, :delete, rule))
      assert redirected_to(conn) == Routes.rule_path(conn, :index)
      assert_error_sent 404, fn ->
        get(conn, Routes.rule_path(conn, :show, rule))
      end
    end
  end

  defp create_rule(_) do
    rule = fixture(:rule)
    {:ok, rule: rule}
  end
end
