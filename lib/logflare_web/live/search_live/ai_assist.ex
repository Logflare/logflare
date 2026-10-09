defmodule LogflareWeb.SearchLive.AiAssist do
  @moduledoc false

  defstruct enabled?: false,
            upgrade_required?: false,
            fields: %{},
            loading?: false,
            request: nil,
            feedback: nil,
            pending_feedback: nil,
            macintosh?: false

  @type feedback :: %{
          natural_language_request: String.t(),
          anthropic_request_id: String.t() | nil,
          submitted?: boolean()
        }

  @type t :: %__MODULE__{
          enabled?: boolean(),
          upgrade_required?: boolean(),
          fields: map(),
          loading?: boolean(),
          request: String.t() | nil,
          feedback: feedback() | nil,
          pending_feedback: feedback() | nil,
          macintosh?: boolean()
        }

  @spec new(String.t() | nil, boolean(), String.t()) :: t()
  def new(user_agent, configured?, plan_name) do
    upgrade_required? = configured? and plan_name in ["Free", "Legacy"]

    %__MODULE__{
      enabled?: configured? and not upgrade_required?,
      upgrade_required?: upgrade_required?,
      macintosh?: is_binary(user_agent) and String.contains?(user_agent, "Macintosh")
    }
  end
end
