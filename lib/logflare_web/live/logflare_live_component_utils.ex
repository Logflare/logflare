defmodule LogflareWeb.LiveComponentUtils do
  def send_assigns_to_self(key, value) do
    send(self(), {:lvc_assigns, key, value})
  end
end
