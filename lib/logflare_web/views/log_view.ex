defmodule LogflareWeb.LogView do
  use LogflareWeb, :view

  def render("index.json", %{message: message}) do
    %{message: message}
  end

end
