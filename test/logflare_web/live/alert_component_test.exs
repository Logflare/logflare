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

  test "close event broadcasts a :clear_flash message with the flash key as an atom" do
    {:noreply, _socket} =
      AlertComponent.handle_event("close", %{"flash_key" => "info"}, %Phoenix.LiveView.Socket{})

    assert_received {:clear_flash, :info}
  end
end
