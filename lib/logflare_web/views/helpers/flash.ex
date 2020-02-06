defmodule LogflareWeb.Helpers.Flash do
  @moduledoc false
  def render_flash(flash, opts \\ [in_live_view: false]) do
    in_live_view = opts[:in_live_view]

    LogflareWeb.LayoutView.render("notifications.html",
      flash: %{
        error: flash[:error],
        info: flash[:warning]
      },
      in_live_view: in_live_view
    )
  end
end
