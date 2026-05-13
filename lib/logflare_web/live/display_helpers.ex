defmodule LogflareWeb.Live.DisplayHelpers do
  @moduledoc false

  @doc """
  Sanitizes a backend configuration map by masking sensitive values while preserving allowed keys.

  ## Examples

      iex> config = %{hostname: "localhost", password: "secret123", port: 5432}
      iex> sanitize_backend_config(config)
      %{hostname: "localhost", password: "**********", port: 5432}

      iex> sanitize_backend_config(nil)
      %{}
  """
  def sanitize_backend_config(config) when is_map(config) do
    allowed_keys =
      ~w(async_insert batch_timeout database hostname insert_protocol native_pool_size native_port pool_size port project_id read_only_url region s3_bucket schema storage_region table url)a

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
