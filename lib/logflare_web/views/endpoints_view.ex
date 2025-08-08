defmodule LogflareWeb.EndpointsView do
  use LogflareWeb, :view

  def render("query.json", %{result: data}) when not is_nil(data) do
    %{result: data}
  end

  def render("query.json", %{errors: errors}) do
    %{errors: errors}
  end
end
