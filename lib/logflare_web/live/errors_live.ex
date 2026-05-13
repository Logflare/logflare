defmodule LogflareWeb.ErrorsLive do
  defmodule InvalidResourceError do
    defexception message: "Resource not found", plug_status: 404
  end
end
