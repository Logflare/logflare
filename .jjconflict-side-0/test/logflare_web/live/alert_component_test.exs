defmodule LogflareWeb.AlertComponentTest do
  use ExUnit.Case
  alias LogflareWeb.AlertComponent
  import Phoenix.LiveViewTest

  test "renders alert component" do
    assert render_component(AlertComponent, %{
             key: "test",
             alert_class: "alert_class",
             id: "alert_test"
           }) =~ "div"
  end
end
