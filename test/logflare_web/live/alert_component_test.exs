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

  test "close event with an unknown flash key does not create an atom" do
    key = "unknown_flash_key_#{System.unique_integer([:positive])}"

    {:noreply, _socket} =
      AlertComponent.handle_event("close", %{"flash_key" => key}, %Phoenix.LiveView.Socket{})

    refute_received {:clear_flash, _}
    assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
  end
end
