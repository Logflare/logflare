defmodule LogflareWeb.QueryComponents do
  use Phoenix.Component

  attr :bytes, :integer, default: nil

  def query_cost(assigns) do
    {size, unit} = humanize_bytes(assigns.bytes)

    assigns =
      assigns
      |> assign(unit: unit)
      |> assign(size: Decimal.from_float(size) |> Decimal.normalize())

    ~H"""
    <div :if={is_number(@bytes)} class="tw-text-sm">
      <%= @size %> <%= @unit %> processed
    </div>
    """
  end

  @doc """
  Converts a bytes count to human readable scale.

  ## Examples

    iex> humanize_bytes(1)
    {1.0, "byte"}

    iex> humanize_bytes(2)
    {2.0, "bytes"}

    iex> humanize_bytes(1024)
    {1.0, "KB"}

    iex> humanize_bytes(1363149)
    {1.3, "MB"}

    iex> humanize_bytes(50 * 1024 * 1024 * 1024)
    {50.0, "GB"}

    iex> humanize_bytes(2 * 1024 * 1024 * 1024 * 1024)
    {2.0, "TB"}

  """
  @spec humanize_bytes(integer) :: {float(), String.t()}
  def humanize_bytes(count) when count == 1, do: {1.0, "byte"}

  def humanize_bytes(count) when is_integer(count) do
    units = ["bytes", "KB", "MB", "GB", "TB"]

    {size, unit} =
      Enum.reduce_while(units, count, fn unit, size ->
        if size >= 1024 do
          {:cont, size / 1024}
        else
          {:halt, {size * 1.0, unit}}
        end
      end)

    {Float.round(size, 2), unit}
  end
end
