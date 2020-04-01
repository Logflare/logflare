defmodule Logflare.Google.BigQuery.GCPConfig do
  @moduledoc """
  Utility functions for Google Cloud configuration
  """
  @default_dataset_location "US"

  def default_project_id() do
    Application.get_env(:logflare, Logflare.Google)[:project_id]
  end

  def dataset_id_append() do
    Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]
  end

  def default_dataset_location() do
    @default_dataset_location
  end
end
