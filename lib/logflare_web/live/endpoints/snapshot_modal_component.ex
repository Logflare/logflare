defmodule LogflareWeb.Endpoints.SnapshotModalComponent do
  use LogflareWeb, :live_component

  alias LogflareWeb.Endpoints.Components

  @impl true
  def render(assigns) do
    assigns =
      assigns
      |> assign(:version_number, get_in(assigns.version.meta, ["version_number"]))

    ~H"""
    <div id="endpoint-version-snapshot-content" class="tw-flex tw-flex-col tw-gap-4 tw-bg-[#161616] tw-p-2">
      <div class="tw-text-lg tw-font-semibold tw-px-4 tw-text-zinc-400">
        Version {@version_number}
      </div>
      <Components.endpoint_settings_panel endpoint={@snapshot} />
      <Components.endpoint_query_panel endpoint={@snapshot} />
    </div>
    """
  end
end
