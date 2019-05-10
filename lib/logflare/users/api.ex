defmodule Logflare.Users.API do
  @callback action_allowed?(map) :: boolean
  alias Logflare.{Users, Sources}
  @api_call_logs {:api_call, :logs_post}

  def action_allowed?(%{type: @api_call_logs} = action) do
    %{source_id: sid, user: user} = action
    sid = String.to_existing_atom(sid)

    source_rate = Sources.get_api_rate_by_id(sid, bucket: :default)
    user_rate = get_user_api_rate(user.id)

    quotas = api_calls_quotas_for(user)

    source_rate <= quotas.source and user_rate <= quotas.user
  end


end
