defmodule LogflareWeb.Helpers.Flash do
  @moduledoc false
  def render_flash(flash, opts \\ []) do
    in_live_view = opts[:in_live_view]

    Phoenix.View.render(LogflareWeb.LayoutView, "notifications.html",
      flash: %{
        error: flash[:error],
        info: flash[:warning]
      },
      in_live_view: in_live_view
    )
  end
end
