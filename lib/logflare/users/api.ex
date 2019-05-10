defmodule Logflare.Users.API do
  @callback action_allowed?(map) :: boolean
  alias Logflare.{Users, Sources}
  @api_call_logs {:api_call, :logs_post}

  def action_allowed?(%{type: @api_call_logs} = action) do
    %{source_id: sid, user: user} = action

    source_rate = Sources.get_api_rate_by_id(sid, bucket: :default)
    user_rate = get_total_user_api_rate(user.id)

    with {:ok, quotas} <- Users.Cache.get_api_quotas(user.id, sid),
         {:source_rate, true} <- {:source_rate, source_rate <= quotas.source},
         {:user_rate, true} <- {:user_rate, user_rate <= quotas.user} do
      :ok
    else
      {:error, reason} -> {:error, reason}
      {:source_rate, false} -> {:error, "Source rate is over the API quota"}
      {:user_rate, false} -> {:error, "User rate is over the API quota"}
    end
  end

  def get_total_user_api_rate(user) do
    user
    |> Users.Cache.list_source_ids()
    |> Enum.map(&Sources.get_api_rate_by_id(&1, bucket: :default))
    |> Enum.sum()
  end
end
