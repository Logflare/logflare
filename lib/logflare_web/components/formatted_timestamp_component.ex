defmodule LogflareWeb.FormattedTimestampComponent do
  @moduledoc """
  Renders a formatted timestamp hint for log event field values.
  """

  use Phoenix.Component

  alias LogflareWeb.Helpers.BqSchema

  @display_format "%Y-%m-%d %H:%M:%S"
  @title_format "%Y-%m-%dT%H:%M:%SZ"

  attr :value, :integer, required: true
  attr :timezone, :string, default: nil

  def formatted_timestamp(%{value: value, timezone: timezone} = assigns) do
    formatted = formatted(value, timezone)

    assigns =
      assigns
      |> assign(:formatted, formatted)
      |> assign(:title, title(value, formatted))

    ~H"""
    <span :if={@formatted} class="tw-ml-2 tw-text-neutral-400" title={@title} data-toggle="tooltip" data-placement="top">
      {@formatted}
    </span>
    """
  end

  defp formatted(timestamp, timezone) when is_integer(timestamp) do
    formatted = BqSchema.format_timestamp(timestamp, timezone, format: @display_format)

    if active_timezone?(timezone), do: formatted, else: formatted <> " UTC"
  end

  defp formatted(_timestamp, _timezone), do: nil

  defp title(timestamp, formatted) when is_integer(timestamp) and is_binary(formatted) do
    BqSchema.format_timestamp(timestamp, "UTC", format: @title_format)
  end

  defp title(_timestamp, _formatted), do: nil

  defp active_timezone?(timezone), do: match?(%Timex.TimezoneInfo{}, Timex.Timezone.get(timezone))
end
