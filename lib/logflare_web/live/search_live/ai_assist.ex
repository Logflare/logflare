defmodule LogflareWeb.SearchLive.AiAssist do
  @moduledoc false

  defstruct enabled?: false,
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
          fields: map(),
          loading?: boolean(),
          request: String.t() | nil,
          feedback: feedback() | nil,
          pending_feedback: feedback() | nil,
          macintosh?: boolean()
        }

  @spec new(String.t() | nil, boolean()) :: t()
  def new(user_agent, enabled?) do
    %__MODULE__{
      enabled?: enabled?,
      macintosh?: is_binary(user_agent) and String.contains?(user_agent, "Macintosh")
    }
  end
end
