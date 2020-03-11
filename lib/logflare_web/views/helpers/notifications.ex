defmodule LogflareWeb.Helpers.Notifications do
  @moduledoc false
  def render_notifications(notifications, opts \\ [in_live_view: false]) do
    in_live_view = opts[:in_live_view]

    LogflareWeb.LayoutView.render("notifications.html",
      notifications: %{
        error: notifications[:error],
        info: notifications[:warning]
      },
      in_live_view: in_live_view
    )
  end
end
