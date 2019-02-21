defmodule LogflareWeb.DashboardChannel do
  use LogflareWeb, :channel

  def join("dashboard:" <> _account_id, payload, socket) do
    if authorized?(payload) do
      {:ok, socket}
    else
      {:error, %{reason: "unauthorized"}}
    end
  end

  # Add authorization logic here as required.
  defp authorized?(_payload) do
    true
  end
end
