defmodule LogflareWeb.Helpers.Flash do
  @moduledoc false
  def render_flash(flash) do
    Phoenix.View.render(LogflareWeb.LayoutView, "notifications.html",
      flash: %{
        error: flash[:error],
        info: flash[:warning]
      }
    )
  end
end
