defmodule Logflare.Logs.Validators.LogParamsNotEmpty do
  @moduledoc """
  Validates that types of values for the same field path are the same
  """

  # Public
  def validate(%{log_event: %{body: body}}) do
    if body[:message] in [%{}, [], "", nil, {}] do
      {:error, message()}
    else
      :ok
    end
  end

  def message() do
    "Log entry needed."
  end
end
