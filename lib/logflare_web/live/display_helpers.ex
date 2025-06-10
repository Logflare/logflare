defmodule LogflareWeb.Live.DisplayHelpers do
  @moduledoc false

  def sanitize_backend_config(%{} = config) do
    allowed_keys =
      ~w(batch_timeout database hostname pool_size port project_id region s3_bucket schema storage_region table url)a

    config
    |> Enum.map(fn {key, value} ->
      if key in allowed_keys do
        {key, value}
      else
        {key, "**********"}
      end
    end)
    |> Map.new()
  end

  def sanitize_backend_config(_config), do: %{}
end
