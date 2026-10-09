defmodule LogflareWeb.FormattedTimestampComponent do
  @moduledoc """
  Renders a timestamp in the selected timezone with a UTC tooltip.
  """

  use Phoenix.Component

  alias LogflareWeb.Helpers.BqSchema

  @display_format "%Y-%m-%d %H:%M:%S"
  @title_format "%Y-%m-%dT%H:%M:%SZ"

  attr :value, :any,
    required: true,
    doc: "Unix timestamp, DateTime, or NaiveDateTime (assumed UTC)"

  attr :timezone, :string, default: nil
  attr :format, :string, default: @display_format
  attr :class, :string, default: "tw-ml-2 tw-text-neutral-400"

  @spec formatted_timestamp(map()) :: Phoenix.LiveView.Rendered.t()
  def formatted_timestamp(%{value: value, timezone: timezone, format: format} = assigns) do
    formatted = formatted(value, timezone, format)

    assigns =
      assigns
      |> assign(:formatted, formatted)
      |> assign(:title, title(value, formatted))

    ~H"""
    <span :if={@formatted} class={@class} title={@title} data-toggle="tooltip" data-placement="top">
      {@formatted}
    </span>
    """
  end

  defp formatted(timestamp, timezone, format)
       when is_integer(timestamp) or is_struct(timestamp, DateTime) or
              is_struct(timestamp, NaiveDateTime) do
    formatted = BqSchema.format_timestamp(timestamp, timezone, format: format)

    if active_timezone?(timezone), do: formatted, else: formatted <> " UTC"
  end

  defp formatted(_timestamp, _timezone, _format), do: nil

  defp title(timestamp, formatted) when is_binary(formatted) do
    BqSchema.format_timestamp(timestamp, "UTC", format: @title_format)
  end

  defp title(_timestamp, _formatted), do: nil

  defp active_timezone?(timezone), do: match?(%Timex.TimezoneInfo{}, Timex.Timezone.get(timezone))
end
