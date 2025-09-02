defmodule LogflareWeb.ModalLiveHelpersTest do
  use ExUnit.Case, async: true
  import Phoenix.LiveViewTest

  alias LogflareWeb.ModalLiveHelpers
  alias Phoenix.LiveView.JS

  @modal_opts [
    id: "test_id",
    key: "test_key",
    alert_class: "error",
    return_to: "/",
    title: "Title"
  ]

  describe "custom click commands" do
    test "live_modal_show_link" do
      opts = [
        component: LogflareWeb.EventContextComponent,
        modal_id: "1",
        title: "Test"
      ]

      assert ModalLiveHelpers.live_modal_show_link("test", opts)
             |> rendered_to_string() =~ ~s|phx-click="show_live_modal"|

      assert ModalLiveHelpers.live_modal_show_link(
               "test",
               opts ++ [click: JS.push("custom_event")]
             )
             |> rendered_to_string() =~ ~r/phx-click=.*custom_event.*show_live_modal/
    end

    test "render the modal with close action" do
      attrs =
        ModalLiveHelpers.live_modal(LogflareWeb.AlertComponent, @modal_opts)

      html = render_component(attrs.component, attrs.assigns)

      assert html =~ ~s|phx-click-away="close"|
      assert html =~ ~s|phx-window-keydown="close"|
      assert html =~ ~s|phx-click="close"|
    end

    test "render the modal with custom close command" do
      attrs =
        ModalLiveHelpers.live_modal(
          LogflareWeb.AlertComponent,
          @modal_opts ++ [close: JS.push("custom_close")]
        )

      html = render_component(attrs.component, attrs.assigns)

      assert html =~ ~r|phx-click-away.*custom_close|
      assert html =~ ~r|phx-window-keydown.*custom_close|
      assert html =~ ~r|phx-click.*custom_close|
    end
  end
end
