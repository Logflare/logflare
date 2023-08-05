defmodule LogflareWeb.EndpointsView do
  use LogflareWeb, :view
  alias Logflare.SingleTenant
  def render("query.json", %{result: data}) do
  # if SingleTenant.supabase_mode?() and SingleTenant.postgres_backend?() do
  #   data = Enum.map(fn
  #     %{"event_message"=> _, "timestamp"=> ts, "id"=> _} = datum->
  #       %{datum | "timestamp" => }
  #     datum ->datum
  #   end)
  #   %{result: data}

  # else
  #   %{result: data}
  # end
    %{result: data}
  end

  def render("query.json", %{error: error}) do
    %{error: error}
  end
end
